WITH osv_with_types AS (
    SELECT
        o.account_balance_date,
        o.second_order_account_number,
        o.account_number,
        o.account_balance_in_amount,
        o.account_balance_out_amount,
        o.account_debit_turn_amount,
        o.account_credit_turn_amount,
        acc_type.C_CODE AS acc_type_code,
        us.C_NAME as abs_user_acc,
        CASE
            WHEN o.account_balance_out_amount > 0 THEN 'DEBIT'
            WHEN o.account_balance_out_amount < 0 THEN 'CREDIT'
            ELSE 'ZERO'
        END as balance_direction,
        ABS(o.account_balance_out_amount) as balance_abs_amount
    FROM osv_account_balance o
    INNER JOIN abs_acc acc ON o.account_nk = bigint(replace(acc.ID,'.0000000000',''))
    INNER JOIN abs_acc_type acc_type ON acc.C_VID = acc_type.ID
    LEFT JOIN abs_user_acc us ON acc.C_OTV = us.ID
),
violation_detection AS (
    SELECT
        *,
        CASE
            -- Активные счета: не должны иметь кредитового сальдо
            WHEN acc_type_code = 'А' AND balance_direction = 'CREDIT'
            THEN 'RED_BALANCE_ACTIVE'
            -- Пассивные счета: не должны иметь дебетового сальдо
            WHEN acc_type_code = 'П' AND balance_direction = 'DEBIT'
            THEN 'RED_BALANCE_PASSIVE'
            -- Активно-пассивные: остаток должен быть нулевым
            WHEN acc_type_code = 'АП' AND balance_direction != 'ZERO'
            THEN 'NON_ZERO_ACTIVE_PASSIVE'
            ELSE 'COMPLIANT'
        END as violation_type,
        -- Определяем критичность нарушения
        CASE
            WHEN acc_type_code IN ('А', 'П') AND balance_abs_amount >= 10000000
            THEN 'КРИТИЧЕСКОЕ'
            WHEN acc_type_code IN ('А', 'П') AND balance_abs_amount >= 1000000
            THEN 'ВЫСОКОЕ'
            WHEN acc_type_code IN ('А', 'П') AND balance_abs_amount >= 100000
            THEN 'СРЕДНЕЕ'
            WHEN acc_type_code = 'АП' AND balance_abs_amount > 0
            THEN 'СРЕДНЕЕ'
            ELSE 'НИЗКОЕ'
        END as severity_level
    FROM osv_with_types
),
-- Отдельная секция для активно-пассивных счетов
active_passive_violations AS (
    SELECT
        'АКТИВНО-ПАССИВНЫЕ СЧЕТА' as section_name,
        account_balance_date,
        second_order_account_number,
        account_number,
        acc_type_code,
        abs_user_acc,
        account_balance_out_amount,
        balance_abs_amount,
        violation_type,
        severity_level,
        'Остаток должен быть нулевым' as violation_description,
        account_debit_turn_amount,
        account_credit_turn_amount
    FROM violation_detection
    WHERE violation_type = 'NON_ZERO_ACTIVE_PASSIVE'
),
-- Секция для активных и пассивных счетов
standard_violations AS (
    SELECT
        CASE
            WHEN acc_type_code = 'A' THEN 'АКТИВНЫЕ СЧЕТА'
            WHEN acc_type_code = 'П' THEN 'ПАССИВНЫЕ СЧЕТА'
        END as section_name,
        account_balance_date,
        second_order_account_number,
        account_number,
        acc_type_code,
        abs_user_acc,
        account_balance_out_amount,
        balance_abs_amount,
        violation_type,
        severity_level,
        CASE
            WHEN violation_type = 'RED_BALANCE_ACTIVE'
            THEN 'Недопустимое кредитовое сальдо на активном счете'
            WHEN violation_type = 'RED_BALANCE_PASSIVE'
            THEN 'Недопустимое дебетовое сальдо на пассивном счете'
        END as violation_description,
        account_debit_turn_amount,
        account_credit_turn_amount
    FROM violation_detection
    WHERE violation_type IN ('RED_BALANCE_ACTIVE', 'RED_BALANCE_PASSIVE')
),
-- Статистика нарушений
violation_summary AS (
    SELECT
        acc_type_code,
        CASE
            WHEN acc_type_code = 'А' THEN 'Активные'
            WHEN acc_type_code = 'П' THEN 'Пассивные'
            WHEN acc_type_code = 'АП' THEN 'Активно-пассивные'
        END as account_type_name,
        COUNT(*) as total_violations,
        SUM(balance_abs_amount) as total_violation_amount,
        AVG(balance_abs_amount) as avg_violation_amount,
        MAX(balance_abs_amount) as max_violation_amount,
        COUNT(CASE WHEN severity_level = 'КРИТИЧЕСКОЕ' THEN 1 END) as critical_count,
        COUNT(CASE WHEN severity_level = 'ВЫСОКОЕ' THEN 1 END) as high_count,
        COUNT(CASE WHEN severity_level = 'СРЕДНЕЕ' THEN 1 END) as medium_count
    FROM violation_detection
    WHERE violation_type != 'COMPLIANT'
    GROUP BY acc_type_code
)
SELECT
    section_name,
    account_balance_date,
    second_order_account_number,
    account_number,
    acc_type_code,
    violation_description,
    severity_level,
    account_balance_out_amount,
    balance_abs_amount,
    account_debit_turn_amount,
    account_credit_turn_amount,
    violation_type
FROM (
    SELECT * FROM active_passive_violations
    UNION ALL
    SELECT * FROM standard_violations
) violations
ORDER BY
    CASE section_name
        WHEN 'АКТИВНО-ПАССИВНЫЕ СЧЕТА' THEN 1
        WHEN 'АКТИВНЫЕ СЧЕТА' THEN 2
        WHEN 'ПАССИВНЫЕ СЧЕТА' THEN 3
    END,
    CASE severity_level
        WHEN 'КРИТИЧЕСКОЕ' THEN 1
        WHEN 'ВЫСОКОЕ' THEN 2
        WHEN 'СРЕДНЕЕ' THEN 3
        ELSE 4
    END,
    balance_abs_amount DESC;
