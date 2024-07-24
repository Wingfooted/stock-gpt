from datetime import timedelta
from dataclasses import dataclass
from typing import Callable


@dataclass
class sql:
    bsa: Callable = lambda date, symbol: f"""
        SELECT dt.date, dt.total_current_assets, dt.investments_and_advances, dt.net_property_and_equipment, dt.period
        FROM balance_sheet_assets dt
        JOIN (
            SELECT MAX(date) AS max_date
            FROM balance_sheet_assets
            WHERE act_symbol = '{symbol}'
              AND date <= '{date}'
        ) AS latest
        ON dt.date = latest.max_date
        WHERE dt.act_symbol = '{symbol}'
          AND dt.date <= '{date}'
        LIMIT 1;
    """
    bsl: Callable = lambda date, symbol: f"""
        SELECT dt.accounts_payable, dt.total_liabilities
        FROM balance_sheet_liabilities dt
        JOIN (
            SELECT MAX(date) AS max_date
            FROM balance_sheet_liabilities
            WHERE act_symbol = '{symbol}'
              AND date <= '{date}'
        ) AS latest
        ON dt.date = latest.max_date
        WHERE dt.act_symbol = '{symbol}'
          AND dt.date <= '{date}'
        LIMIT 1;
    """
    bse: Callable = lambda date, symbol: f"""
        SELECT dt.retained_earnings
        FROM balance_sheet_equity dt
        JOIN (
            SELECT MAX(date) AS max_date
            FROM balance_sheet_equity
            WHERE act_symbol = '{symbol}'
              AND date <= '{date}'
        ) AS latest
        ON dt.date = latest.max_date
        WHERE dt.act_symbol = '{symbol}'
          AND dt.date <= '{date}'
        LIMIT 1;
    """
    cf: Callable = lambda date, symbol: f"""
        SELECT
            dt.date,
            dt.period,
            dt.cash_at_beginning_of_period,
            dt.net_cash_from_investing_activities,
            dt.net_cash_from_financing_activities,
            dt.net_cash_from_operating_activities,
            dt.net_change_from_assets,
            dt.other_financing_activities,
            dt.investments,
            dt.cash_at_end_of_period,
            dt.net_income,
            dt.other_operating_activities
        FROM cash_flow_statement dt
        JOIN (
            SELECT MAX(date) AS max_date
            FROM cash_flow_statement
            WHERE act_symbol = '{symbol}'
              AND date <= '{date}'
        ) AS latest
        ON dt.date = latest.max_date
        WHERE dt.act_symbol = '{symbol}'
          AND dt.date <= '{date}'
        LIMIT 1;
    """
    inc: Callable = lambda date, symbol: f"""
        SELECT
            dt.date,
            dt.period,
            dt.cost_of_goods,
            dt.selling_administrative_depreciation_amortization_expenses,
            dt.gross_profit
        FROM income_statement dt
        JOIN (
            SELECT MAX(date) AS max_date
            FROM income_statement
            WHERE act_symbol = '{symbol}'
              AND date <= '{date}'
        ) AS latest
        ON dt.date = latest.max_date
        WHERE dt.act_symbol = '{symbol}'
          AND dt.date <= '{date}'
        LIMIT 1;
    """
    # 1_month | 2_month | 3_month | 6_month | 1_year | 2_year | 3_year | 5_year | 7_year | 10_year | 20_year | 30_year
    rates: Callable = lambda date, symbol: f"""
        SELECT
            us.`date`,
            us.`1_month`,
            us.`2_month`,
            us.`3_month`,
            us.`6_month`,
            us.`1_year`,
            us.`2_year`,
            us.`3_year`,
            us.`5_year`,
            us.`7_year`,
            us.`10_year`,
            us.`20_year`,
            us.`30_year`
        FROM us_treasury us
        JOIN (
            SELECT MAX(date) as max_date
            FROM us_treasury
            WHERE date <= '{date}'
        ) AS latest
        ON us.date = latest.max_date
        WHERE us.date <= '{date}'
        LIMIT 1;
    """

    opt: Callable = lambda date, symbol: f"""
        SELECT
            opt.`date`,
            opt.`expiration`,
            opt.`strike`,
            opt.`call_put`,
            opt.`bid`,
            opt.`ask`,
            opt.`vol`, opt.`delta`, opt.`gamma`,
            opt.`theta`, opt.`vega`, opt.`rho`
        FROM option_chain opt
        JOIN (
            SELECT MAX(date) as max_date
            FROM option_chain
            WHERE date <= '{date}' AND act_symbol = '{symbol}'
        ) AS latest
        ON opt.date = latest.max_date
        WHERE opt.date <= '{date}'
        LIMIT 1;
    """

    vh: Callable = lambda date, symbol: f"""
        SELECT
            opt.`date`,
            opt.`hv_current`,
            opt.`hv_week_ago`,
            opt.`hv_month_ago`,
            opt.`hv_year_high`,
            opt.`hv_year_high_date`,
            opt.`hv_year_low`,
            opt.`hv_year_low_date`,
            opt.`iv_current`,
            opt.`iv_week_ago`,
            opt.`iv_month_ago`,
            opt.`iv_year_high`,
            opt.`iv_year_high_date`,
            opt.`iv_year_low`,
            opt.`iv_year_low_date`
        FROM volatility_history opt
        JOIN (
            SELECT MAX(date) as max_date
            FROM volatility_history
            WHERE date <= '{date}' AND act_symbol = '{symbol}'
        ) AS latest
        ON opt.date = latest.max_date
        WHERE opt.date <= '{date}'
        LIMIT 1;
    """

    div: Callable = lambda date, symbol: f"""
        SELECT
            div.`ex_date`,
            div.`amount`
        FROM dividend div
        JOIN (
            SELECT MAX(ex_date) as max_date
            FROM dividend
            WHERE ex_date <= '{date}' AND act_symbol = '{symbol}'
        ) AS latest
        ON div.ex_date = latest.max_date
        WHERE div.ex_date <= '{date}'
        LIMIT 1;
    """

    today = lambda date, symbol: f"""
        SELECT
            open,
            high,
            low,
            close,
            volume
        FROM
            ohlcv
        WHERE
            act_symbol = '{symbol}'
            AND date = '{date}';
        """

    future = lambda date, symbol: f"""
        SELECT
            open,
            high,
            low,
            close,
            volume
        FROM
            ohlcv
        WHERE
            act_symbol = '{symbol}'
            AND date = '{(date + timedelta(days=1 if date.weekday() < 4 else 3 if date.weekday() == 4 else 2))}';
    """
