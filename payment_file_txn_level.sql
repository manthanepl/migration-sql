
SELECT *
FROM
  (WITH dpd_identification_data AS
     (SELECT customer_id,
             vendor,
             CASE
                 WHEN max_dpd_bucket=1 THEN '0-30'
                 WHEN max_dpd_bucket=2 THEN '31-60'
                 WHEN max_dpd_bucket=3 THEN '61-90'
                 WHEN max_dpd_bucket=4 THEN '91-120'
                 WHEN max_dpd_bucket=5 THEN '121-150'
                 WHEN max_dpd_bucket=6 THEN '151-180'
                 WHEN max_dpd_bucket=7 THEN 'above 180'
             END AS dpd_bucket
      FROM
        (SELECT customer_id,
                vendor,
                min(rank_given) AS max_dpd_bucket
         FROM
           (SELECT customer_id,
                   CASE
                       WHEN dpd_bucket='0-30' THEN 1
                       WHEN dpd_bucket='31-60' THEN 2
                       WHEN dpd_bucket='61-90' THEN 3
                       WHEN dpd_bucket='91-120' THEN 4
                       WHEN dpd_bucket='121-150' THEN 5
                       WHEN dpd_bucket='151-180' THEN 6
                       WHEN dpd_bucket='above 180' THEN 7
                   END AS rank_given,
                   CASE
                       WHEN OWNER='501' THEN 'Credigenics'
                       WHEN OWNER='502' THEN 'Resolution'
                       WHEN OWNER='503' THEN 'Paymac'
                       WHEN OWNER='504' THEN 'Om Sai Credit'
                       WHEN OWNER='505' THEN 'SV Enterprises'
                       WHEN OWNER='506' THEN 'Debt Care'
                       WHEN OWNER='507' THEN 'Riddhi Siddhi'
                       WHEN OWNER='508' THEN 'Vinayak Service'
                       WHEN OWNER='510' THEN 'Saviiour'
                       WHEN OWNER='511' THEN 'The Orion'
                       WHEN OWNER='512' THEN 'Pavi Agency'
                       WHEN OWNER='513' THEN 'Royalty Service Mumbai'
                       WHEN OWNER='514' THEN 'Intellectual Financial'
                       WHEN OWNER='515' THEN 'Sanvi Financial'
                       WHEN OWNER='516' THEN 'Shri Sai Solution'
                       WHEN OWNER='517' THEN 'M S Associates'
                       WHEN OWNER='518' THEN 'Jaithara Enterprises'
                       WHEN OWNER='519' THEN 'Shivaay Enterprise'
                       WHEN OWNER = '520' THEN 'TruBoard'
                       when owner = '521' then 'Collekto'
when owner = '523' then 'CLXN'
         when owner = '524' then 'SHVV'
         when owner = '525' then 'Unique Recoveries and Management services'
         when owner = '526' then '361 Degree solutions'
         when owner = '527' then 'Fastserve'
                  when owner = '528' then 'Charbhuja Enterprise'
         when owner = '530' then 'Otofin'
when owner = '531' then 'Resolution Management'
when owner = '532' then 'Morya'
when owner = '533' then 'Winfinix'
when owner = '534' then 'Bahaar'
when owner = '535' then 'Vision'
when owner = '536' then 'Dignity'
when owner = '537' then 'M H Collection'
when owner = '538' then 'Gurman'
when owner = '539' then 'Dhriti'
when owner = '540' then 'arkfinserv'
when owner = '541' then 'Khshitij enterprises'
when owner = '542' then 'panindia'
                   END AS vendor
            FROM bronze.datamart.collection_assignment_history
            LEFT JOIN bronze.retail.team ON collection_assignment_history.owner=team.id
            WHERE assignment_date::Date>=date_format(CURRENT_TIMESTAMP - interval '1 day', 'yyyy-MM-01')
              AND assignment_date::Date<=((CURRENT_TIMESTAMP +interval'330 minutes'- interval '1 day')::date)
 AND collection_assignment_history.owner IN ('501',
                                                          '502',
                                                          '503',
                                                          '504',
                                                          '505',
                                                          '506',
                                                          '507',
                                                          '508',
                                                          '510',
                                                          '511',
                                                          '512',
                                                          '513',
                                                          '514',
                                                          '515',
                                                          '516',
                                                          '517',
                                                          '518',
                                                          '519',
                                                          '520','521','523','524','525','526','527','528','530','531','532','533','534','535','536','537','538','539','540','541',
                                                          '542')
              AND worst_dpd>2 ) t
         GROUP BY customer_id,
                  vendor) p),
        assignment_data AS
     (SELECT collection_assignment_history.customer_id,
             CASE
                 WHEN OWNER='501' THEN 'Credigenics'
                 WHEN OWNER='502' THEN 'Resolution'
                 WHEN OWNER='503' THEN 'Paymac'
                 WHEN OWNER='504' THEN 'Om Sai Credit'
                 WHEN OWNER='505' THEN 'SV Enterprises'
                 WHEN OWNER='506' THEN 'Debt Care'
                 WHEN OWNER='507' THEN 'Riddhi Siddhi'
                 WHEN OWNER='508' THEN 'Vinayak Service'
                 WHEN OWNER='510' THEN 'Saviiour'
                 WHEN OWNER='511' THEN 'The Orion'
                 WHEN OWNER='512' THEN 'Pavi Agency'
                 WHEN OWNER='513' THEN 'Royalty Service Mumbai'
                 WHEN OWNER='514' THEN 'Intellectual Financial'
                 WHEN OWNER='515' THEN 'Sanvi Financial'
                 WHEN OWNER='516' THEN 'Shri Sai Solution'
                 WHEN OWNER='517' THEN 'M S Associates'
                 WHEN OWNER='518' THEN 'Jaithara Enterprises'
                 WHEN OWNER='519' THEN 'Shivaay Enterprise'
                 WHEN OWNER = '520' THEN 'TruBoard'
                                        when owner = '521' then 'Collekto'
when owner = '523' then 'CLXN'
         when owner = '524' then 'SHVV'
         when owner = '525' then 'Unique Recoveries and Management services'
         when owner = '526' then '361 Degree solutions'
         when owner = '527' then 'Fastserve'
         when owner = '528' then 'Charbhuja Enterprise'
         when owner = '530' then 'Otofin'
when owner = '531' then 'Resolution Management'
when owner = '532' then 'Morya'
when owner = '533' then 'Winfinix'
when owner = '534' then 'Bahaar'
when owner = '535' then 'Vision'
when owner = '536' then 'Dignity'
when owner = '537' then 'M H Collection'
when owner = '538' then 'Gurman'
when owner = '539' then 'Dhriti'
when owner = '540' then 'arkfinserv'
when owner = '541' then 'Khshitij enterprises'
when owner = '542' then 'panindia'
             END AS vendor,
             OWNER,
             dpd_identification_data.dpd_bucket,
             min(assignment_date::Date) AS min_assigned_date,
             max((CURRENT_TIMESTAMP+interval '330 minutes')::Date) AS max_assigned_date,
             max(coalesce(total_payable_amount::float,0)) AS max_allocated_amount
      FROM bronze.datamart.collection_assignment_history
      LEFT JOIN dpd_identification_data ON collection_assignment_history.customer_id=dpd_identification_data.customer_id
 WHERE assignment_date::Date>=date_format(CURRENT_TIMESTAMP - interval '1 day', 'yyyy-MM-01')
        AND assignment_date::Date<=((CURRENT_TIMESTAMP +interval'330 minutes'- interval '1 day')::date)
        AND collection_assignment_history.owner IN ('501',
                                                    '502',
                                                    '503',
                                                    '504',
                                                    '505',
                                                    '506',
                                                    '507',
                                                    '508',
                                                    '510',
                                                    '511',
                                                    '512',
                                                    '513',
                                                    '514',
                                                    '515',
                                                    '516',
                                                    '517',
                                                    '518',
                                                    '519',
                                                    '520','521','523','524','525','526','527','528','530','531','532','533','534','535','536','537','538','539','540','541',
                                                    '542')
        AND worst_dpd>2
      GROUP BY collection_assignment_history.customer_id,
               OWNER,
               CASE
                   WHEN OWNER='501' THEN 'Credigenics'
                   WHEN OWNER='502' THEN 'Resolution'
                   WHEN OWNER='503' THEN 'Paymac'
                   WHEN OWNER='504' THEN 'Om Sai Credit'
                   WHEN OWNER='505' THEN 'SV Enterprises'
                   WHEN OWNER='506' THEN 'Debt Care'
                   WHEN OWNER='507' THEN 'Riddhi Siddhi'
                   WHEN OWNER='508' THEN 'Vinayak Service'
                   WHEN OWNER='510' THEN 'Saviiour'
                   WHEN OWNER='511' THEN 'The Orion'
                   WHEN OWNER='512' THEN 'Pavi Agency'
                   WHEN OWNER='513' THEN 'Royalty Service Mumbai'
                   WHEN OWNER='514' THEN 'Intellectual Financial'
                   WHEN OWNER='515' THEN 'Sanvi Financial'
                   WHEN OWNER='516' THEN 'Shri Sai Solution'
                   WHEN OWNER='517' THEN 'M S Associates'
                   WHEN OWNER='518' THEN 'Jaithara Enterprises'
                   WHEN OWNER='519' THEN 'Shivaay Enterprise'
                   WHEN OWNER = '520' THEN 'TruBoard'
                                          when owner = '521' then 'Collekto'
when owner = '523' then 'CLXN'
         when owner = '524' then 'SHVV'
         when owner = '525' then 'Unique Recoveries and Management services'
         when owner = '526' then '361 Degree solutions'
         when owner = '527' then 'Fastserve'
         when owner = '528' then 'Charbhuja Enterprise'
         when owner = '530' then 'Otofin'
when owner = '531' then 'Resolution Management'
when owner = '532' then 'Morya'
when owner = '533' then 'Winfinix'
when owner = '534' then 'Bahaar'
when owner = '535' then 'Vision'
when owner = '536' then 'Dignity'
when owner = '537' then 'M H Collection'
when owner = '538' then 'Gurman'
when owner = '539' then 'Dhriti'
when owner = '540' then 'arkfinserv'
when owner = '541' then 'Khshitij enterprises'
when owner = '542' then 'panindia'
               END,
               dpd_identification_data.dpd_bucket),
        payment_data AS
     (SELECT customer_id,
             payment_date,
             sum(amount_received) AS amount_received,
             txn_id,
             status,
payment_status,
             settlement_status
      FROM
        (SELECT epl_marketplace_customer.customer_id,
                (epl_transaction_payments.created+interval '330 minutes')::Date AS payment_date,
                sum(coalesce(epl_transaction_payments.txn_amount,0) + coalesce(epl_transaction_payments.txn_fees,0)) AS amount_received,
                epl_transaction_payments.txn_id,
                CASE
                    WHEN epl_transaction.status IN ('settled',
                                                    'settled_final',
                                                    'returned',
                                                    'partial_refund_pending') THEN 'Closed'
                    WHEN epl_transaction.status = 'partially_settled' THEN 'Partially Recovered'
                    ELSE epl_transaction.status
                END AS status,
                CASE
                    WHEN payment_status IN ('Success',
                                            'authorized',
                                            'rpSettled',
                                            'captured',
                                            'CHARGED') THEN 'successful'
                END AS payment_status,
                settlement_status
         FROM bronze.online.epl_transaction_payments
         LEFT JOIN bronze.online.epl_transaction ON epl_transaction_payments.txn_id=epl_transaction.id
         LEFT JOIN bronze.online.epl_marketplace_customer ON epl_transaction.marketplace_customer_id=epl_marketplace_customer.id
         WHERE lower(payment_status) IN ('success',
                                         'rpsettled',
                                         'charges',
                                         'captured',
                                         'authorized',
                                         'charged',
                                         'rip',
                                         'sip')
           AND lower(settlement_status) NOT IN ('refunded',
                                                'reversed',
                                                'cancelled')
           AND (epl_transaction_payments.created+interval '330 minutes')::Date>=(to_char((CURRENT_TIMESTAMP-interval '1 days' ),'yyyy-MM-01'))
           AND (epl_transaction_payments.created+interval '330 minutes')::Date<=((CURRENT_TIMESTAMP +interval'330 minutes'- interval '1 day')::date)
           AND  DATEDIFF(
    (epl_transaction_payments.created + INTERVAL '330 minutes')::DATE,
    CASE
        WHEN epl_transaction.marketplace_id = 632 THEN 
            (epl_transaction.due_date + INTERVAL '330 minutes' - INTERVAL '16 days')::DATE
        ELSE 
            (epl_transaction.due_date + INTERVAL '330 minutes')::DATE
    END
)>2
         GROUP BY epl_marketplace_customer.customer_id,
                  epl_transaction_payments.txn_id,
                  (epl_transaction_payments.created+interval '330 minutes')::Date,
                  CASE
                      WHEN epl_transaction.status IN ('settled',
                                                      'settled_final',
                                                      'returned',
                                                      'partial_refund_pending') THEN 'Closed'
                      WHEN epl_transaction.status = 'partially_settled' THEN 'Partially Recovered'
                      ELSE epl_transaction.status
                  END,
                  CASE
                      WHEN payment_status IN ('Success',
                                              'authorized',
                                              'rpSettled',
                                              'captured',
                                              'CHARGED') THEN 'successful'
                  END,
                  settlement_status
         UNION SELECT epl_marketplace_customer.customer_id,
                      (epl_transaction_payments.created+interval '330 minutes')::Date AS payment_date,
                      sum(coalesce(epl_transaction_payments.txn_amount,0) + coalesce(epl_transaction_payments.txn_fees,0)) AS amount_received,
                      epl_transaction_payments.txn_id,
                      CASE
                          WHEN epl_transaction.status IN ('settled',
                                                          'settled_final',
                                                          'returned',
                                                          'partial_refund_pending') THEN 'Closed'
                          WHEN epl_transaction.status = 'partially_settled' THEN 'Partially Recovered'
                          ELSE epl_transaction.status
  END AS status,
                      CASE
                          WHEN payment_status IN ('Success',
                                                  'authorized',
                                                  'rpSettled',
                                                  'captured',
                                                  'CHARGED') THEN 'successful'
                      END AS payment_status,
                      settlement_status
         FROM bronze.online.epl_transaction_payments
         LEFT JOIN bronze.online.epl_transaction ON epl_transaction_payments.txn_id=epl_transaction.id
         LEFT JOIN bronze.online.epl_marketplace_customer ON epl_transaction.marketplace_customer_id=epl_marketplace_customer.id
         WHERE lower(payment_status) IN ('success',
                                         'rpsettled',
                                         'charges',
                                         'captured',
                                         'authorized',
                                         'charged',
                                         'rip',
                                         'sip')
           AND lower(settlement_status) NOT IN ('refunded',
                                                'reversed',
                                                'cancelled')
           AND (epl_transaction_payments.created+interval '330 minutes')::Date>=(TO_char((CURRENT_TIMESTAMP  - interval '1 day'), 'yyyy-MM-01'))
           AND (epl_transaction_payments.created+interval '330 minutes')::Date<=((CURRENT_TIMESTAMP +interval'330 minutes' - interval '1 day')::date)
           AND  DATEDIFF(
    (epl_transaction_payments.created + INTERVAL '330 minutes')::DATE,
    CASE
        WHEN epl_transaction.marketplace_id = 632 THEN 
            (epl_transaction.due_date + INTERVAL '330 minutes' - INTERVAL '16 days')::DATE
        ELSE 
            (epl_transaction.due_date + INTERVAL '330 minutes')::DATE
    END
)<=2 --   and lower(epl_transaction_payments.payment_method) not in ('emandate','enach','nach','upimandate')

           AND (epl_transaction.txn_date+interval '330 minutes')::date!=(epl_transaction_payments.created+interval '330 minutes')::Date
         GROUP BY epl_marketplace_customer.customer_id,
                  epl_transaction_payments.txn_id,
                  (epl_transaction_payments.created+interval '330 minutes')::Date,
                  CASE
                      WHEN epl_transaction.status IN ('settled',
                                                      'settled_final',
                                                      'returned',
                                                      'partial_refund_pending') THEN 'Closed'
                      WHEN epl_transaction.status = 'partially_settled' THEN 'Partially Recovered'
                      ELSE epl_transaction.status
                  END,
                  CASE
                      WHEN payment_status IN ('Success',
                                              'authorized',
                                              'rpSettled',
                                              'captured',
                                              'CHARGED') THEN 'successful'
                  END,
                  settlement_status) t
      GROUP BY customer_id,
               payment_date,
               txn_id,
               status,
               payment_status,
               settlement_status),
        to_take_payment_data AS
     (SELECT customer_id,
             telephone_number,
             txn_id AS transaction_id,
             amount_received AS total_payment_amount,
             status,
             due_date,
             payment_status,
             settlement_status,
             payment_date,
             vendor,
             OWNER,
             dpd_bucket
 FROM
        (SELECT payment_data.*,
                epl_customer.telephone_number,
                dpd_identification_data.dpd_bucket,
                assignment_data.vendor,
                (epl_transaction.due_date + interval '330 minutes')::date AS due_date,
                assignment_data.owner,
                assignment_data.max_assigned_date,
                assignment_data.min_assigned_date,
                CASE
                    WHEN payment_data.payment_date>=assignment_data.min_assigned_date
                         AND payment_data.payment_date<=assignment_data.max_assigned_date THEN 'yes'
                    ELSE 'no'
                END AS pick_flag
         FROM payment_data
         LEFT JOIN assignment_data ON payment_data.customer_id=assignment_data.customer_id
         LEFT JOIN dpd_identification_data ON dpd_identification_data.customer_id = assignment_data.customer_id
         LEFT JOIN bronze.online.epl_customer ON epl_customer.id = payment_data.customer_id
         LEFT JOIN bronze.online.epl_transaction ON epl_transaction.id = payment_data.txn_id) t
      WHERE pick_flag='yes' ) SELECT *
   FROM to_take_payment_data)t
WHERE OWNER IN ('%s')