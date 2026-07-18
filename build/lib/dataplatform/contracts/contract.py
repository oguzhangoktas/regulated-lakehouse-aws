"""Contract enforcement at a dataset boundary.

A contract declares the schema and the rules a dataset must satisfy before it is
handed to a consumer. It is data rather than code: the YAML is the source of truth
and this evaluates it, so a rule cannot drift away from its declaration.

Two kinds of check, with different consequences:

  row rules           evaluated per row. A failing row is quarantined carrying the
                      names of the rules it broke; the rest of the dataset publishes.
  dataset assertions  evaluated over the whole dataset. A failure means the dataset
                      is wrong as a whole, so nothing publishes.
"""
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

import yaml
from pyspark.sql import Column, DataFrame, functions as F


class ContractViolation(Exception):
    """A dataset-level assertion failed. The dataset must not be published."""


@dataclass(frozen=True)
class Contract:
    name: str
    version: str
    grain: list[str]
    fields: list[dict]
    row_rules: dict[str, str]
    assertions: dict

    @classmethod
    def load(cls, path: str | Path) -> "Contract":
        return cls._from_yaml(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def _from_yaml(cls, text: str) -> "Contract":
        spec = yaml.safe_load(text)
        return cls(
            name=spec["contract"],
            version=spec["version"],
            grain=spec["grain"],
            fields=spec["fields"],
            row_rules=spec.get("row_rules", {}),
            assertions=spec.get("dataset_assertions", {}),
        )

    @classmethod
    def named(cls, name: str) -> "Contract":
        """Load a contract shipped with the package.

        Resolves through the package rather than the filesystem, so a job finds its
        contract the same way from a checkout and from the wheel Glue loads.
        """
        resource = files("dataplatform.contracts") / f"{name}.yaml"
        return cls._from_yaml(resource.read_text(encoding="utf-8"))

    @property
    def field_names(self) -> list[str]:
        return [field["name"] for field in self.fields]

    def _rules(self) -> list[tuple[str, Column]]:
        """Rules implied by the field declarations, then the declared row rules."""
        rules: list[tuple[str, Column]] = []

        for field in self.fields:
            name = field["name"]
            column = F.col(name)

            if field.get("required"):
                rules.append((f"{name}_present", column.isNotNull()))
            if "min" in field:
                rules.append((f"{name}_min", column >= field["min"]))
            if "allowed" in field:
                allowed = column.isin(field["allowed"])
                # An optional field carries no value to check against the list; a
                # missing required one is already caught above.
                if not field.get("required"):
                    allowed = column.isNull() | allowed
                rules.append((f"{name}_allowed", allowed))

        rules.extend((name, F.expr(expr)) for name, expr in self.row_rules.items())
        return rules

    def enforce(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Split df into rows that satisfy every rule and rows that do not.

        Raises if the frame does not carry the declared fields: that is a schema
        break rather than a data defect, and no row-level outcome is meaningful.
        """
        missing = set(self.field_names) - set(df.columns)
        if missing:
            raise ContractViolation(
                f"{self.name} {self.version}: missing fields {sorted(missing)}"
            )

        # Project to the contract, so a field the contract does not declare cannot
        # reach the consumer.
        projected = df.select(*self.field_names)

        broken = F.array_compact(
            F.array(*[F.when(~cond, F.lit(name)) for name, cond in self._rules()])
        )
        tagged = projected.withColumn("failed_rules", broken)

        passed = tagged.filter(F.size("failed_rules") == 0).drop("failed_rules")
        quarantined = tagged.filter(F.size("failed_rules") > 0)
        return passed, quarantined

    def assert_dataset(self, passed: DataFrame, quarantined: DataFrame) -> None:
        """Raise if the dataset as a whole is unfit to publish."""
        if self.assertions.get("unique_grain"):
            total = passed.count()
            distinct = passed.select(*self.grain).distinct().count()
            if total != distinct:
                raise ContractViolation(
                    f"{self.name} {self.version}: {total - distinct} rows duplicate "
                    f"the grain {self.grain}"
                )

        limit = self.assertions.get("max_quarantine_pct")
        if limit is not None:
            kept, dropped = passed.count(), quarantined.count()
            total = kept + dropped
            share = dropped / total * 100 if total else 0.0
            if share > limit:
                raise ContractViolation(
                    f"{self.name} {self.version}: {share:.2f}% of rows quarantined, "
                    f"limit is {limit}%"
                )
