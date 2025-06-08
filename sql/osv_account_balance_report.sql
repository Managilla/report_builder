WITH cdm_acc_balance AS (
    SELECT
        abf.account_nk
        , acc.C_MAIN_V_ID as account_number
        , substr(acc.C_MAIN_V_ID,1,5) as acc_num_bs2
        , abf.account_balance_effective_from_date
        , abf.account_balance_effective_to_date
        , abf.account_balance_in_amount
        , abf.account_balance_in_currency_amount
        , abf.account_debit_turn_amount
        , abf.account_debit_turn_currency_amount
        , abf.account_credit_turn_amount
        , abf.account_credit_turn_currency_amount
        , abf.account_balance_out_amount
        , abf.account_balance_out_currency_amount
        , abf.closed_date_flag
    FROM account_balance abf
    INNER JOIN abs_acc acc
        ON bigint(replace(acc.ID,'.0000000000','')) = abf.account_nk
    WHERE to_date(abf.account_balance_effective_from_date) < {cutoff_day}
        AND abf.valid_to_date = to_date('2100-12-31')
)
, records AS (
    SELECT
        bigint(replace(acc.ID,'.0000000000','')) AS account_nk
        , bigint(replace(rec.ID,'.0000000000','')) AS record_nk
        , acc.C_MAIN_V_ID as account_number
        , substr(acc.C_MAIN_V_ID,1,5) as acc_num_bs2
        , rec.C_SUMMA as record_amount
        , to_date(C_DATE) as record_date
        , rec.C_DATE as record_dttm
        , CASE
            WHEN rec.C_DT = 1 THEN TRUE
            ELSE FALSE
          END as record_debit_flag
        , rec.C_START_SUM as record_in_balance_amount
        , rec.C_SUMMA_NAT as record_rub_amount
        , rec.C_START_SUM_NAT as record_rub_in_balance_amount
        , rec.C_STAMP as record_stamp_dttm
        , bigint(replace(rec.COLLECTION_ID,'.0000000000','')) AS collection_id
        , bigint(replace(rec.C_ACC_CORR,'.0000000000','')) AS corr_account_nk
        , bigint(replace(rec.C_DOC,'.0000000000','')) AS posting_nk
    FROM abs_records rec
    INNER JOIN abs_acc acc
        ON acc.C_ARC_MOVE = rec.COLLECTION_ID
    WHERE to_date(rec.C_DATE) >= {cutoff_day}

    ORDER BY rec.C_DATE ASC
)
, account_balance AS (
    SELECT
        rec.account_nk
        , account_number
        , acc_num_bs2
        , rec.record_date                         AS account_balance_effective_from_date
        , COALESCE(LEAD(rec.record_date) OVER (PARTITION BY rec.account_nk ORDER BY rec.record_date) - 1,
                 to_date('2100-12-31'))                AS account_balance_effective_to_date
        , rec.account_balance_in_amount
        , rec.account_balance_in_currency_amount
        , rec.account_debit_turn_amount
        , rec.account_debit_turn_currency_amount
        , rec.account_credit_turn_amount
        , rec.account_credit_turn_currency_amount
        , rec.account_balance_in_amount + rec.account_debit_turn_amount +
          rec.account_credit_turn_amount          AS account_balance_out_amount
        , rec.account_balance_in_currency_amount + rec.account_debit_turn_currency_amount +
          rec.account_credit_turn_currency_amount AS account_balance_out_currency_amount
        , CASE
              WHEN jod.`C_CHANGE_POSS#0` = 3
                  THEN TRUE
              ELSE FALSE
          END                                     AS closed_date_flag
    FROM (
        SELECT
            account_nk
            , record_nk
            , account_number
            , acc_num_bs2
            , record_date
            , record_dttm
            , record_in_balance_amount                        AS account_balance_in_currency_amount
            , record_rub_in_balance_amount                    AS account_balance_in_amount
            , -1 * SUM(CASE
                         WHEN record_debit_flag IS TRUE
                             THEN record_rub_amount
                         ELSE 0
                     END)
                  OVER (PARTITION BY account_nk, record_date) AS account_debit_turn_amount
            , -1 * SUM(CASE
                         WHEN record_debit_flag IS TRUE
                             THEN record_amount
                         ELSE 0
                     END)
                  OVER (PARTITION BY account_nk, record_date) AS account_debit_turn_currency_amount
            , SUM(CASE
                     WHEN record_debit_flag IS FALSE
                         THEN record_rub_amount
                     ELSE 0
                 END)
              OVER (PARTITION BY account_nk, record_date)      AS account_credit_turn_amount
            , SUM(CASE
                     WHEN record_debit_flag IS FALSE
                         THEN record_amount
                     ELSE 0
                 END)
              OVER (PARTITION BY account_nk, record_date)      AS account_credit_turn_currency_amount
            , ROW_NUMBER() OVER (
                PARTITION BY account_nk, record_date
                ORDER BY record_dttm, record_stamp_dttm, record_nk) AS rn
        FROM records
    )                                        rec
    LEFT JOIN journal_op_days jod
       ON rec.record_date = jod.C_OP_DAY
    WHERE rec.rn = 1
)
, union_acc_balance AS (
    SELECT
        account_nk
        , account_number
        , acc_num_bs2
        , account_balance_effective_from_date
        , account_balance_effective_to_date
        , account_balance_in_amount
        , account_balance_in_currency_amount
        , account_debit_turn_amount
        , account_debit_turn_currency_amount
        , account_credit_turn_amount
        , account_credit_turn_currency_amount
        , account_balance_out_amount
        , account_balance_out_currency_amount
        , closed_date_flag
    FROM cdm_acc_balance
    UNION ALL
    SELECT
        account_nk
        , account_number
        , acc_num_bs2
        , account_balance_effective_from_date
        , account_balance_effective_to_date
        , account_balance_in_amount
        , account_balance_in_currency_amount
        , account_debit_turn_amount
        , account_debit_turn_currency_amount
        , account_credit_turn_amount
        , account_credit_turn_currency_amount
        , account_balance_out_amount
        , account_balance_out_currency_amount
        , closed_date_flag
    FROM account_balance
)
SELECT
    date_add(current_date(), -1) AS account_balance_date
    , acc_num_bs2      AS second_order_account_number
    , account_number
    , CASE
          WHEN account_balance_effective_from_date = date_add(current_date(), -1)
              THEN account_balance_in_amount
          ELSE account_balance_out_amount
      END              AS account_balance_in_amount
    , account_balance_out_amount
    , CASE
          WHEN account_balance_effective_from_date = date_add(current_date(), -1)
              THEN account_debit_turn_amount
          ELSE 0
      END              AS account_debit_turn_amount
    , CASE
          WHEN account_balance_effective_from_date = date_add(current_date(), -1)
              THEN account_credit_turn_amount
          ELSE 0
      END              AS account_credit_turn_amount
FROM union_acc_balance
WHERE date_add(current_date(), -1) BETWEEN account_balance_effective_from_date AND account_balance_effective_to_date
;
