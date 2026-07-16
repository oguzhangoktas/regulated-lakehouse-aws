# Source data: LendingClub accepted loans (2007-2018Q4)

2,260,701 rows, 151 columns, 1.6GB CSV. Reduced to the 27 columns used
(platform/ingestion/build_loan_master.py) and written as Parquet at 127MB.

## Columns used

Immutable at origination: id, issue_d, term, loan_amnt, funded_amnt, int_rate,
installment, grade, sub_grade, purpose, addr_state, annual_inc, dti,
application_type, home_ownership, emp_length, verification_status, fico_range_low,
fico_range_high.

Final observed outcome, used to derive the trajectory (ADR-005): loan_status,
out_prncp, total_pymnt, total_rec_prncp, last_pymnt_d, recoveries.

## Columns excluded

zip_code, emp_title, url, title, desc: identify the borrower. Not carried past
build_loan_master.py. customer_id is a hash of the loan id.

member_id: 100% null across the file. LendingClub removed it.

policy_code, pymnt_plan, disbursement_method: single value across the file.

sec_app_*, *_joint, revol_bal_joint: populated only for joint applications, which are
a small minority.

## Distributions (full scan, not sampled)

grade: A 19.15%, B 29.35%, C 28.75%, D 14.35%, E 6.00%, F 1.85%, G 0.54%.
Reasonably balanced across seven values. Partitioning or salting on grade is not
warranted.

loan_status: Fully Paid 47.63%, Current 38.85%, Charged Off 11.88%,
Late (31-120 days) 0.95%, In Grace Period 0.37%, Late (16-30 days) 0.19%,
Does not meet the credit policy (Fully Paid 0.09%, Charged Off 0.03%), Default 0.00%.

The literal Default status covers 40 rows. Impairment in this portfolio is
Charged Off, at 268,559 rows. Defining default on the Default status alone would
mis-state the book. See ADR-003.

Rows per issue year: 603 in 2007 rising to 495,242 in 2018, a factor of roughly 820.
Any partitioning on issue date produces partitions of very uneven size.

Null rates on the columns used are effectively zero (dti is the highest at 0.08%).
The file has been cleaned before publication and does not carry the quality problems a
live source feed has.

## Constraints

Date granularity: issue_d and last_pymnt_d carry month and year only ("Dec-2018").
They are parsed to the first of the month, so derived days past due moves in monthly
steps and the population sitting in the 1-89 day band shifts between month ends. The
source has no day component to recover.

Extract boundary: the file ends in December 2018. Loans whose last payment falls in
that month have not had time to accrue days past due, so the past due population at
2018-12-31 is understated.

Sampling: the file is ordered by issue date. A head sample covers only the earliest
vintages and reports distributions that do not hold for the file
(platform/profiling/scan_credit.py reads it in full for this reason).
