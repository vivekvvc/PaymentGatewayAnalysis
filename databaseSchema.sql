create database paymentanalytics;

use paymentanalytics;

create table ordertransactionlog(
    transactionid varchar(36),
    orderid varchar(36),
    paymentstatus varchar(100),
    amount float,
    tran_createddate datetime,
    tran_updateddate datetime,
    payment_method varchar(100),
    isretry boolean,
    paymentgateway varchar(20)
);

--Test sql:
select * from ordertransactionlog;
with cte_transactionstatus as
(
select orderid, payment_method, dense_rank() over(partition by orderid order by tran_createddate ) as rnk,
amount,paymentstatus,paymentgateway
from ordertransactionlog ),
cte_retryinfo as
(select * from cte_transactionstatus where rnk <4 order by orderid)
select paymentgateway,
     payment_method,
     CASE WHEN rnk=1 THEN 'Attempt1'
          WHEN rnk=2 THEN 'Retry1'
          WHEN rnk=3 THEN 'Retry2' END as Attempts,
	COUNT(distinct (CASE WHEN paymentstatus='settled' then orderid else NULL END)) as success,
    COUNT(distinct (CASE WHEN paymentstatus<>'settled' then orderid else NULL END)) as Failed
from cte_retryinfo
group by paymentgateway,payment_method,rnk
UNION ALL
SELECT 'BrainTree' as paymentgateway, 'venmo_account' as payment_method,'Attempt1' AS attempts, 0 as success, 0 as failed;


select
     paymentgateway,
     payment_method,
     count( distinct (CASE WHEN rnk=1  and paymentstatus='settled' then orderid else NULL END)) as attempt1_success,
     count( distinct (CASE WHEN rnk=1  and paymentstatus <>'settled' then orderid else NULL END)) as attempt1_failed,
     count( distinct (CASE WHEN rnk=2 and paymentstatus='settled' then orderid else NULL END)) as retry1_success,
     count( distinct (CASE WHEN rnk=2 and paymentstatus<>'settled' then orderid else NULL END)) as retry1_failed,
	count( distinct (CASE WHEN rnk=3  and paymentstatus='settled' then orderid else NULL END)) as retry2_success,
    count( distinct (CASE WHEN rnk=3  and paymentstatus<>'settled' then orderid else NULL END)) as retry2_failed
from cte_retryinfo
group by paymentgateway,payment_method ;