
-- Loan Table
with loan_table as (
SELECT
	fl.DWH_LOAN_ID AS asset_id,
	fl.DWH_CUST_ID AS borrower_id,
	fl.loan_date AS issue_date,
	fl.loan_date AS investment_date,
	CASE WHEN fl.Country = 'Kenya' OR stations = 'Jiinue' THEN 'KES' 
       WHEN fl.Country =  'Uganda' THEN 'UGX' 
       ELSE NULL END AS  asset_currency,
	fl.product,
	loan_amount AS asset_amount,
	loan_amount - LEAST(coll_all, loan_amount) AS outstanding_principal_balance,
	fl.loan_due_date AS maturity_date,
	'Full amortization' AS amortisation_type,
	'Fixed' AS interest_rate_type,
	loan_interest AS interest_rate_exp,
	initiation_fee,
	greatest(loan_late_interest,(penalty_one + penalty_two + penalty_three)) penalties,
	risk_score AS current_rating,
	'Internal' AS rating_source,
	CASE WHEN coll_90 < loan_total_interest AND current_date >= LAST_DAY(DATE_ADD(fl.loan_date, INTERVAL 90 DAY)) THEN LAST_DAY(DATE_ADD(fl.loan_date, INTERVAL 90 DAY))  
		ELSE NULL		
	END AS default_date,
	default_balance AS default_amount, 
	CASE WHEN coll_120 < loan_total_interest AND current_date >= LAST_DAY(DATE_ADD(fl.loan_date, INTERVAL 120 DAY)) THEN LAST_DAY(DATE_ADD(fl.loan_date, INTERVAL 120 DAY))
		ELSE NULL
	END AS write_off_date,
	write_off_balance AS write_off_amount,
	CASE WHEN settlementdate IS NOT NULL THEN date(settlementdate)
		WHEN fl.write_off_date IS NOT NULL THEN fl.write_off_date
		WHEN -- if settled but date blank pull from loan payment table pick the last date where the payment type is collection THEN latest loan_rep_date //for Claud//
					(fl.final_loan_status in ('Settled','Closed Repaid') or coll_all >= loan_total_interest+early_settlement_surplus) and settlementdate is null then max(fp.date) over(partition by fp.dwh_loan_code)
 		ELSE NULL
	END AS closing_date,
	
	coll_pre_woff,loan_total_interest,coll_dd,coll_pre_default,curr_balance,coll_all,default_balance,early_settlement_surplus , waiver,
FROM `dwh-4g.dwh_4g.FactLoan` fl
left join `dwh_4g.FactPayments` fp on fp.dwh_loan_code = fl.dwh_loan_code
LEFT JOIN `merlin_binlog_dump.merlin_customer_status` mcs ON fl.final_loan_status = CAST(mcs.id AS string)
WHERE fl.loan_date BETWEEN '2023-01-01' AND current_date
AND fl.product IN ('Kuza', 'Upia')),

loan_table_2 as (select distinct *,	

	CASE WHEN write_off_date IS NOT NULL THEN 'Written-off'
		WHEN closing_date > default_date AND coll_pre_woff >= loan_total_interest+early_settlement_surplus THEN 'Settled after default'
		WHEN closing_date is not null AND coll_dd >= loan_total_interest+early_settlement_surplus  THEN 'Settled on time'
		WHEN closing_date is null AND coll_dd >= loan_total_interest+early_settlement_surplus  THEN 'Settled on time'
		WHEN closing_date > maturity_date AND coll_pre_default >= loan_total_interest+early_settlement_surplus THEN 'Settled past due date'
		WHEN current_date <= maturity_date AND coll_dd >= loan_total_interest- early_settlement_surplus  THEN 'Settled on time'
		WHEN closing_date is null AND coll_dd < loan_total_interest+early_settlement_surplus and coll_all >= loan_total_interest  THEN 'Settled past due date'
		WHEN current_date > maturity_date AND coll_all <= loan_total_interest+early_settlement_surplus THEN 'Delinquent'
		WHEN default_balance > 0 THEN 'Default'
		ELSE 'Performing'
	END AS performance_status,

CASE WHEN current_date <= maturity_date THEN NULL -- loan has not yet matured
		WHEN closing_date > maturity_date THEN date_diff( closing_date,maturity_date, day) -- loan settled past maturity date
		WHEN write_off_date is not null THEN date_diff(write_off_date,maturity_date, day) -- loan written off past maturity date
		WHEN closing_date is NULL AND write_off_date is NULL AND current_date > maturity_date THEN date_diff( current_date,maturity_date, day) -- loan past maturity date not settled AND not writeoff
		ELSE NULL -- loan settled on time or ahead of maturity date
	END AS days_in_delay,
	'actual/actual' AS day_count_convention

	 from loan_table)

select *,
CASE WHEN performance_status IN('Written-off','Settled after default','Settled past due date','Settled on time') AND closing_date IS NOT NULL THEN 'Closed'
		WHEN performance_status IN('Default','Delinquent','Performing') THEN 'Open'
		ELSE NULL
	END AS asset_status,
 from loan_table_2
	--  where 
	-- closing_date is not null and 
	--  performance_status = 'Performing'
	--  performance_status = 'ERROR'

-- )
where issue_date between '2023-01-01' and '2023-03-31' -- Loans_data-2023_Jan-Mar-20240202
-- where issue_date between '2023-04-01' and '2023-06-30' -- Loans_data-2023_Apr-Jun-20240202
-- where issue_date between '2023-07-01' and '2023-09-30' -- Loans_data-2023_Jul-Sep-20240202
-- where issue_date between '2023-10-01' and '2023-12-31' -- Loans_data-2023_Oct-Dec-20240202
-- where issue_date between '2024-01-01' and '2024-01-31' -- Loans_data-2024_Jan-20240202
ORDER BY issue_date ASC
