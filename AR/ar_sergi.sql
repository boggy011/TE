-- Databricks notebook source
-- DBTITLE 1,ar_sergi temp view
create or replace temp view ar_sergi as
WITH f_accounts_receivable_invoice_cte AS (
  select
    bukrs,
    kunnr,
    belnr,
    gjahr,
    buzei,
    blart,
    bldat,
    budat,
    augbl,
    augdt,
    shkzg,
    xzahl,
    vbeln,
    sgtxt,
    zfbdt,
    zterm,
    zbd1t,
    zbd2t,
    zbd3t,
    waers,
    wrbtr,
    dmbtr,
    rstgr,
    saknr,
    'OPEN' item_status
  from
    finance_conf.sapPR2028__bsid
  union all
  select
    bukrs,
    kunnr,
    belnr,
    gjahr,
    buzei,
    blart,
    bldat,
    budat,
    augbl,
    augdt,
    shkzg,
    xzahl,
    vbeln,
    sgtxt,
    zfbdt,
    zterm,
    zbd1t,
    zbd2t,
    zbd3t,
    waers,
    wrbtr,
    dmbtr,
    rstgr,
    saknr,
    'CLEARED' item_status
  from
    finance_conf.sapPR2028__bsad
),
d_t052_distinct_cte AS (
  select
    zterm,
    max(try_cast(ztag1 as integer)) ztag1
  from
    master_data_conf.sapPR2028__t052
  group by
    all
),
d_material_cte AS (
  select
    vbeln,
    concat_ws(', ', collect_list(distinct matnr)) material_number
  from
    direct_sales_conf.sapPR2028__vbrp
  group by
    all
),
d_tariff_percentages_cte AS (
  select
    bseg.belnr belnr,
    bseg.bukrs bukrs,
    bseg.gjahr gjahr,
    max(vbrp.matnr) matnr
  from
    finance_conf.sapPR2028__bseg bseg
      left join direct_sales_conf.sapPR2028__vbrp vbrp
        on vbrp.vbeln = bseg.kidno
        and vbrp.matnr in ('1-120037-2')
        and vbrp.AUBEL = bseg.VBEL2
        and vbrp.AUPOS = bseg.POSN2
  group by
    all
),
d_tariff_konv_cte AS (
  select
    knumv,
    max(kbetr) kbetr
  from
    pricing_l0_raw.sapPR2028__konv
  where
    kschl in ('ZTRF', 'ZTRM')
  group by
    all
),
-- [ABAP-PC-OVERRIDE] CBC resolution chain
ref_gbl_product AS (
  select
    PROD_CODE,
    PROD_BUSLN_FNCTN_ID
  from
    master_data_conf.gbl_current__gbl_product
  where
    coalesce(PROD_BUSLN_FNCTN_ID, '') != ''
),
ref_cbc_by_billing_doc AS (
  select
    vbrp.vbeln,
    gp.PROD_BUSLN_FNCTN_ID as competency_business_cde,
    row_number() over (partition by vbrp.vbeln order by abs(vbrp.netwr) desc) as rn
  from
    direct_sales_conf.sapPR2028__vbrp vbrp
      inner join master_data_l0_raw.sapPR2028__mara mara
        on vbrp.matnr = mara.matnr
      inner join ref_gbl_product gp
        on mara.prdha = gp.PROD_CODE
),
-- [SOLD-TO-CHAIN] MODULE: Sold-To + WWW resolution (6 CTEs)
-- Tier 2: VBFA → VBAK direct chain
sd_soldto_direct_cte AS (
  select
    bukrs,
    belnr,
    gjahr,
    buzei,
    soldto_kunnr_from_sd_direct
  from
    (
      select
        ar.bukrs,
        ar.belnr,
        ar.gjahr,
        ar.buzei,
        vbak.kunnr as soldto_kunnr_from_sd_direct,
        row_number() over (
            partition by ar.bukrs, ar.belnr, ar.gjahr, ar.buzei
            order by vbfa.vbelv
          ) as rn
      from
        f_accounts_receivable_invoice_cte ar
          inner join direct_sales_l0_raw.sapPR2028__vbfa vbfa
            on vbfa.vbeln = ar.vbeln
            and vbfa.vbtyp_v in ('C', 'H', 'B', 'E', 'F', 'G', 'K', 'L', 'I')
          inner join direct_sales_l0_raw.sapPR2028__vbak vbak
            on vbak.vbeln = vbfa.vbelv
            and vbak.bukrs_vf
              = case
                when ar.bukrs = '1082' then '0048'
                else ar.bukrs
              end
      where
        coalesce(trim(ar.vbeln), '') <> ''
    )
  where
    rn = 1
),
-- Tier 3: VBFA delivery → order → VBAK fallback
sd_soldto_delivery_fallback_cte AS (
  select
    bukrs,
    belnr,
    gjahr,
    buzei,
    soldto_kunnr_from_sd_fallback
  from
    (
      select
        ar.bukrs,
        ar.belnr,
        ar.gjahr,
        ar.buzei,
        vbak.kunnr as soldto_kunnr_from_sd_fallback,
        row_number() over (
            partition by ar.bukrs, ar.belnr, ar.gjahr, ar.buzei
            order by
              case
                when vbfa_order.vbtyp_v = 'E' then 1
                when vbfa_order.vbtyp_v = 'C' then 2
                else 9
              end,
              vbfa_order.vbelv
          ) as rn
      from
        f_accounts_receivable_invoice_cte ar
          inner join direct_sales_l0_raw.sapPR2028__vbfa vbfa_delivery
            on vbfa_delivery.vbeln = ar.vbeln
            and vbfa_delivery.vbtyp_v = 'J'
            and coalesce(vbfa_delivery.vbtyp_n, '') <> 'X'
          inner join direct_sales_l0_raw.sapPR2028__vbfa vbfa_order
            on vbfa_order.vbeln = vbfa_delivery.vbelv
            and vbfa_order.vbtyp_v in ('E', 'C')
          inner join direct_sales_l0_raw.sapPR2028__vbak vbak
            on vbak.vbeln = vbfa_order.vbelv
      where
        coalesce(trim(ar.vbeln), '') <> ''
    )
  where
    rn = 1
),
-- 0048 BSEG customer refresh (ABAP 003ANL)
bseg_customer_for_0048_cte AS (
  select
    bukrs,
    belnr,
    gjahr,
    bseg_customer_id
  from
    (
      select
        bukrs,
        belnr,
        gjahr,
        kunnr as bseg_customer_id,
        row_number() over (partition by bukrs, belnr, gjahr order by buzei) as rn
      from
        finance_conf.sapPR2028__bseg
      where
        koart = 'D'
        and coalesce(trim(kunnr), '') <> ''
        and bukrs = '0048'
    )
  where
    rn = 1
),
-- [SOLD-TO-CHAIN v4] Pre-computed "any BUKRS" customer key lookup.
-- One row per (account_number), deterministic-lowest by (company_code, channel).
-- Materialized once; used as a plain LEFT JOIN in soldto_resolution_cte to
-- rescue cross-BUKRS Sold-To extensions where neither original nor 1082 match.
-- Diagnostic confirmed WWW level 1/2 is consistent across extensions, so a
-- deterministic pick is safe.
ref_customer_any_bukrs_cte AS (
  select
    submitted_customer_account_number as account_number,
    customer_key_id
  from
    master_data_l1_curated.dim_customer_current
  where
    source_system_id = 1
  qualify
    row_number() over (
        partition by submitted_customer_account_number
        order by customer_company_code asc, distribution_channel_code asc
      ) = 1
),
-- [SOLD-TO-CHAIN] Step 1: Sold-To resolution + customer_key_id (3 tiers)
soldto_resolution_cte AS (
  select
    ar.bukrs,
    ar.belnr,
    ar.gjahr,
    ar.buzei,
    -- 4-tier Sold-To resolution with 0048 BSEG override
    case
      when
        ar.bukrs = '0048'
        and bseg_0048.bseg_customer_id is not null
      then
        bseg_0048.bseg_customer_id
      else
        coalesce(
          nullif(trim(vbrk.kunag), ''),
          sd_direct.soldto_kunnr_from_sd_direct,
          sd_fb.soldto_kunnr_from_sd_fallback,
          ar.kunnr
        )
    end as sold_to_customer_number,
    case
      when
        ar.bukrs = '0048'
        and bseg_0048.bseg_customer_id is not null
      then
        'ABAP_003ANL_BSEG_0048_REFRESH'
      when nullif(trim(vbrk.kunag), '') is not null then 'BSEG_KIDNO_TO_VBRK_KUNAG'
      when sd_direct.soldto_kunnr_from_sd_direct is not null then 'VBFA_VBAK_DIRECT'
      when sd_fb.soldto_kunnr_from_sd_fallback is not null then 'VBFA_VBAK_DELIVERY_FALLBACK'
      else 'ORIGINAL_BSID_BSAD_KUNNR'
    end as sold_to_resolution_source,
    case
      when ar.bukrs = '0048' then '1082'
      else ar.bukrs
    end as mapped_company_code,
    -- Which tier resolved the customer_key_id (for monitoring)
    case
      when cust_lookup_primary.customer_key_id is not null then 'PRIMARY_BUKRS'
      when cust_lookup_0048_fallback.customer_key_id is not null then 'FALLBACK_0048_TO_1082'
      when any_bukrs.customer_key_id is not null then 'FALLBACK_ANY_BUKRS'
      else 'UNRESOLVED'
    end as customer_key_resolution_tier,
    -- Debug columns
    vbrk.kunag as debug_soldto_from_vbrk,
    sd_direct.soldto_kunnr_from_sd_direct as debug_soldto_from_vbfa_direct,
    sd_fb.soldto_kunnr_from_sd_fallback as debug_soldto_from_vbfa_delivery,
    bseg_0048.bseg_customer_id as debug_soldto_from_0048_bseg,
    -- Resolved key (primary → 0048 fallback → any BUKRS fallback)
    coalesce(
      cust_lookup_primary.customer_key_id,
      cust_lookup_0048_fallback.customer_key_id,
      any_bukrs.customer_key_id
    ) as resolved_customer_key_id
  from
    f_accounts_receivable_invoice_cte ar
      left join finance_conf.sapPR2028__bseg f_bseg_stw
        on ar.belnr = f_bseg_stw.belnr
        and ar.bukrs = f_bseg_stw.bukrs
        and ar.gjahr = f_bseg_stw.gjahr
        and ltrim('0', ar.buzei) = ltrim('0', f_bseg_stw.buzei)
      left join direct_sales_raw.sapPR2028__vbrk vbrk
        on vbrk.vbeln = f_bseg_stw.kidno
      left join sd_soldto_direct_cte sd_direct
        on sd_direct.bukrs = ar.bukrs
        and sd_direct.belnr = ar.belnr
        and sd_direct.gjahr = ar.gjahr
        and sd_direct.buzei = ar.buzei
      left join sd_soldto_delivery_fallback_cte sd_fb
        on sd_fb.bukrs = ar.bukrs
        and sd_fb.belnr = ar.belnr
        and sd_fb.gjahr = ar.gjahr
        and sd_fb.buzei = ar.buzei
      left join bseg_customer_for_0048_cte bseg_0048
        on bseg_0048.bukrs = ar.bukrs
        and bseg_0048.belnr = ar.belnr
        and bseg_0048.gjahr = ar.gjahr
      -- Tier 1: primary lookup under original BUKRS
      LEFT JOIN LATERAL (
        SELECT
          customer_key_id
        FROM
          master_data_l1_curated.dim_customer_current
        WHERE
          submitted_customer_account_number
            = case
              when
                ar.bukrs = '0048'
                and bseg_0048.bseg_customer_id is not null
              then
                bseg_0048.bseg_customer_id
              else
                coalesce(
                  nullif(trim(vbrk.kunag), ''),
                  sd_direct.soldto_kunnr_from_sd_direct,
                  sd_fb.soldto_kunnr_from_sd_fallback,
                  ar.kunnr
                )
            end
          AND customer_company_code = ar.bukrs
          AND source_system_id = 1
        ORDER BY
          distribution_channel_code ASC
        LIMIT 1
      ) cust_lookup_primary
      -- Tier 2: for 0048 rows only, also search under '1082'
      LEFT JOIN LATERAL (
        SELECT
          customer_key_id
        FROM
          master_data_l1_curated.dim_customer_current
        WHERE
          submitted_customer_account_number
            = case
              when
                ar.bukrs = '0048'
                and bseg_0048.bseg_customer_id is not null
              then
                bseg_0048.bseg_customer_id
              else
                coalesce(
                  nullif(trim(vbrk.kunag), ''),
                  sd_direct.soldto_kunnr_from_sd_direct,
                  sd_fb.soldto_kunnr_from_sd_fallback,
                  ar.kunnr
                )
            end
          AND customer_company_code = '1082'
          AND source_system_id = 1
          AND ar.bukrs = '0048'
        ORDER BY
          distribution_channel_code ASC
        LIMIT 1
      ) cust_lookup_0048_fallback
      -- Tier 3: any BUKRS where the Sold-To is extended (plain LEFT JOIN, not LATERAL)
      left join ref_customer_any_bukrs_cte any_bukrs
        on any_bukrs.account_number
          = case
            when
              ar.bukrs = '0048'
              and bseg_0048.bseg_customer_id is not null
            then
              bseg_0048.bseg_customer_id
            else
              coalesce(
                nullif(trim(vbrk.kunag), ''),
                sd_direct.soldto_kunnr_from_sd_direct,
                sd_fb.soldto_kunnr_from_sd_fallback,
                ar.kunnr
              )
          end
),
-- [SOLD-TO-CHAIN] Step 2: WWW enrichment from dim_customer_current
soldto_www_cte AS (
  select
    sr.bukrs,
    sr.belnr,
    sr.gjahr,
    sr.buzei,
    sr.sold_to_customer_number,
    sr.sold_to_resolution_source,
    sr.mapped_company_code,
    sr.customer_key_resolution_tier,
    sr.debug_soldto_from_vbrk,
    sr.debug_soldto_from_vbfa_direct,
    sr.debug_soldto_from_vbfa_delivery,
    sr.debug_soldto_from_0048_bseg,
    -- WWW hierarchy (resolved off Sold-To)
    d_csa.customer_key_id as soldto_customer_key_id,
    d_csa.sales_organization_id,
    d_csa.distribution_channel_code,
    d_csa.worldwide_customer_level_1_number,
    d_csa.worldwide_customer_level_1_label as worldwide_customer_level_1_name,
    d_csa.worldwide_customer_level_2_number,
    d_csa.worldwide_customer_level_2_label as worldwide_customer_level_2_name,
    d_csa.country_region_level_1_label as country_region_level_1_name,
    d_csa.country_region_level_2_label as country_region_level_2_name,
    d_csa.gam_level_3_assoc_full_name,
    d_csa.account_manager_level_1_assoc_full_name as account_manager_assoc_full_name,
    d_csa.account_manager_level_6_assoc_full_name,
    d_csa.account_manager_level_7_assoc_full_name,
    d_csa.sales_territory_level_1_assoc_full_name as sales_territory_assoc_full_name,
    d_csa.sales_territory_level_6_assoc_full_name,
    d_csa.sales_territory_level_7_assoc_full_name,
    d_csa.sales_office_code,
    d_csa.sales_office_label as sales_office_name
  from
    soldto_resolution_cte sr
      left join master_data_l1_curated.dim_customer_current d_csa
        on d_csa.customer_key_id = sr.resolved_customer_key_id
),
-- [SOLD-TO-CHAIN] END MODULE
ar_base AS (
  select
    f_act_rec_inv_cte.bukrs company_code,
    f_act_rec_inv_cte.kunnr customer_account_number,
    f_act_rec_inv_cte.belnr accounting_document_id,
    f_act_rec_inv_cte.gjahr accounting_document_posting_fiscal_year_id,
    f_act_rec_inv_cte.buzei accounting_document_item_id,
    f_act_rec_inv_cte.blart accounting_document_type_code,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.bldat
    ) accounting_document_create_date,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.budat
    ) accounting_document_posting_date,
    f_act_rec_inv_cte.augbl accounting_document_item_clearing_document_id,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.augdt
    ) accounting_document_item_clearing_date,
    f_act_rec_inv_cte.shkzg accounting_document_item_credit_or_debit_code,
    f_act_rec_inv_cte.xzahl accounting_document_item_posting_key_used_in_payment_indicator,
    f_act_rec_inv_cte.vbeln accounting_document_item_invoice_document_id,
    f_act_rec_inv_cte.sgtxt accounting_document_item_text,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.zfbdt
    ) accounting_document_item_baseline_date,
    date_add(
      finance_tbl.convert_sap_datestring_to_date(
        if(
          coalesce(f_act_rec_inv_cte.zfbdt, '') in ('', '00000000'),
          f_act_rec_inv_cte.bldat,
          f_act_rec_inv_cte.zfbdt
        )
      ),
      case
        when
          f_act_rec_inv_cte.shkzg = 'H'
          and coalesce(f_bseg.rebzg, '') = ''
        then
          0
        else
          coalesce(
            nullif(try_cast(f_act_rec_inv_cte.zbd3t as integer), 0),
            nullif(try_cast(f_act_rec_inv_cte.zbd2t as integer), 0),
            nullif(try_cast(f_act_rec_inv_cte.zbd1t as integer), 0),
            d_t052_cte.ztag1,
            0
          )
      end
    ) accounting_document_item_due_date,
    f_act_rec_inv_cte.zterm accounting_document_item_payment_terms_code,
    f_act_rec_inv_cte.zbd1t accounting_document_item_cash_discount_days_quantity_1,
    f_act_rec_inv_cte.zbd2t accounting_document_item_cash_discount_days_quantity_2,
    f_bkpf.usnam accounting_document_create_network_user_id,
    f_act_rec_inv_cte.waers accounting_document_item_document_currency_code,
    f_t001.waers accounting_document_item_functional_currency_code,
    f_act_rec_inv_cte.rstgr accounting_document_item_payment_reason_code,
    f_act_rec_inv_cte.item_status accounting_document_item_clearing_status_name,
    coalesce(f_bfod.prctr, f_bseg.prctr) as profit_center_id,
    d_ted_profit_center.ztedprc ted_profit_center_id,
    finance_tbl.convert_sap_datestring_to_date(
      d_ted_profit_center.eff_date
    ) ted_profit_center_effective_date,
    f_bfod.auzei accounting_document_breakdown_item_id,
    if(
      coalesce(f_bfod.auzei, 1) = 1,
      finance_tbl.round_and_set_signal_amount(f_act_rec_inv_cte.wrbtr, f_act_rec_inv_cte.shkzg),
      0
    ) accounting_document_item_document_currency_amount,
    f_act_rec_inv_cte.saknr accouting_document_item_general_ledger_reconciliation_account_number,
    round(
      coalesce(
        finance_tbl.round_and_set_signal_amount(f_bfod.dmbtr, f_bfod.shkzg),
        finance_tbl.round_and_set_signal_amount(f_act_rec_inv_cte.dmbtr, f_act_rec_inv_cte.shkzg)
      )
        * power(10, 2 - coalesce(f_tcurx_functional.currdec, 2)),
      3
    ) accounting_document_item_functional_currency_amount,
    case
      when
        d_tk_cte.kbetr is not null
      then
        coalesce(d_tk_cte.kbetr / 10, if(d_tp_cte.matnr is not null, 100, 0))
      when d_tp_cte.matnr is not null then 100
      else 0
    end accounting_document_item_tariff_percentage,
    d_cs.customer_owning_business_unit_dsg_id,
    d_cs.sales_territory_owning_business_unit_dsg_id,
    d_cs.gam_global_business_unit_owning_business_unit_dsg_id,
    d_dc.company_dsg_id,
    d_cs.industry_business_code,
    f_t001.waers as company_currency_code,
    d_cer.currency_exchange_rate_to_USD_multiplier_factor,
    d_curr.exchange_rate,
    d_curr.from_currency_ratio,
    d_curr.target_currency_ratio,
    d_cbc.competency_business_cde as resolved_cbc,
    d_pc_rels.MGE_PROFIT_CENTER_ABBREV_ID as override_bu_abbrev,
    d_pc_rels.SAP_PROFIT_CENTER_CDE as override_sap_profit_center,
    d_bu.business_unit_id as initial_business_unit_id,
    d_bu.business_unit_dsg_id,
    d_bu.business_unit_group_id,
    -- [SOLD-TO-CHAIN] Sold-To resolution + 0048 mapping + traceability
    stw.sold_to_customer_number,
    stw.sold_to_resolution_source,
    stw.mapped_company_code,
    stw.customer_key_resolution_tier,
    stw.debug_soldto_from_vbrk,
    stw.debug_soldto_from_vbfa_direct,
    stw.debug_soldto_from_vbfa_delivery,
    stw.debug_soldto_from_0048_bseg,
    -- [SOLD-TO-CHAIN] WWW columns
    stw.soldto_customer_key_id as customer_key_id,
    stw.sales_organization_id,
    stw.distribution_channel_code,
    stw.worldwide_customer_level_1_number,
    stw.worldwide_customer_level_1_name,
    stw.worldwide_customer_level_2_number,
    stw.worldwide_customer_level_2_name,
    stw.country_region_level_1_name,
    stw.country_region_level_2_name,
    stw.gam_level_3_assoc_full_name,
    stw.account_manager_level_6_assoc_full_name,
    stw.account_manager_level_7_assoc_full_name,
    stw.sales_territory_level_6_assoc_full_name,
    stw.sales_territory_level_7_assoc_full_name,
    stw.sales_office_code,
    stw.sales_office_name,
    stw.account_manager_assoc_full_name,
    stw.sales_territory_assoc_full_name,
    d_cs.sensors_alternate_sales_territory_code,
    d_cs.sensors_alternate_sales_territory_name
  from
    f_accounts_receivable_invoice_cte f_act_rec_inv_cte
      left join finance_conf.sapPR2028__bseg f_bseg
        on f_act_rec_inv_cte.belnr = f_bseg.belnr
        and ltrim('0', f_act_rec_inv_cte.buzei) = ltrim('0', f_bseg.buzei)
        and f_act_rec_inv_cte.bukrs = f_bseg.bukrs
        and f_act_rec_inv_cte.gjahr = f_bseg.gjahr
      left join finance_conf.sapPR2028__bkpf f_bkpf
        on f_act_rec_inv_cte.bukrs = f_bkpf.bukrs
        and f_act_rec_inv_cte.belnr = f_bkpf.belnr
        and f_act_rec_inv_cte.gjahr = f_bkpf.gjahr
      left join master_data_conf.sapPR2028__t001 f_t001
        on f_act_rec_inv_cte.bukrs = f_t001.bukrs
      left join finance_l0_raw.sapPR2028__tcurx f_tcurx_functional
        on f_tcurx_functional.currkey = f_t001.waers
      left join finance_l0_raw.sapPR2028__tcurx f_tcurx_document
        on f_tcurx_document.currkey = f_act_rec_inv_cte.waers
      left join finance_tbl.dim_currencies_dates_rates d_curr
        on d_curr.reference_date
          = finance_tbl.convert_sap_datestring_to_date(
            coalesce(nullif(f_act_rec_inv_cte.augdt, 0), f_act_rec_inv_cte.budat)
          )
        and d_curr.from_currency = f_t001.waers
      left join d_t052_distinct_cte d_t052_cte
        on f_act_rec_inv_cte.zterm = d_t052_cte.zterm
      left join direct_sales_raw.sapPR2028__vbrk vbrk
        on vbrk.vbeln = f_bseg.kidno
      left join d_tariff_konv_cte d_tk_cte
        on d_tk_cte.knumv = vbrk.knumv
      left join d_tariff_percentages_cte d_tp_cte
        on f_bkpf.belnr = d_tp_cte.belnr
        and f_bkpf.bukrs = d_tp_cte.bukrs
        and f_bkpf.gjahr = d_tp_cte.gjahr
      left join master_data_tbl.dim_customer d_cs
        on d_cs.customer_account_number = regexp_replace(f_act_rec_inv_cte.kunnr, '^0+', '')
        and d_cs.customer_company_code = f_act_rec_inv_cte.bukrs
        and d_cs.source_system_id = 1
        and d_cs.customer_company_legacy_format_account_number_rank = 1
        and d_cs.record_active_indicator = 'Y'
      left join master_data_tbl.dim_company d_dc
        on d_dc.company_code = f_act_rec_inv_cte.bukrs
        and d_dc.record_active_indicator = 'Y'
      left join finance_l0_raw.sapPR2028__bfod_a f_bfod
        on f_bfod.belnr = f_act_rec_inv_cte.belnr
        and f_bfod.buzei = f_act_rec_inv_cte.buzei
        and f_bfod.gjahr = f_act_rec_inv_cte.gjahr
        and f_bfod.bukrs = f_act_rec_inv_cte.bukrs
      left join master_data_tbl.dim_profit_center d_pc
        on d_pc.profit_center_id = coalesce(f_bfod.prctr, f_bseg.prctr)
        and d_pc.record_active_indicator = 'Y'
      left join master_data_tbl.dim_business_unit d_bu
        on d_pc.business_unit_id = d_bu.business_unit_id
        and d_bu.record_active_indicator = 'Y'
      left join master_data_tbl.dim_currency_exchange_rate d_cer
        on d_cer.currency_code = f_t001.waers
        and d_cer.currency_exchange_rate_type_code = 1
        and to_date(FROM_UNIXTIME(UNIX_TIMESTAMP())) between
          to_date(d_cer.effective_from_date)
        and
          to_date(d_cer.effective_to_date)
      left join finance_l0_raw.sapPR2028__zpcafence d_ted_profit_center
        on d_ted_profit_center.prctr = coalesce(f_bfod.prctr, f_bseg.prctr)
        and d_ted_profit_center.eff_date <= f_act_rec_inv_cte.budat
      left join ref_cbc_by_billing_doc d_cbc
        on f_bseg.kidno = d_cbc.vbeln
        and d_cbc.rn = 1
        and coalesce(f_bseg.kidno, '') != ''
      left join master_data_conf.gbl_current__gbl_mge_profit_center_rels d_pc_rels
        on f_act_rec_inv_cte.bukrs = d_pc_rels.ORGANIZATION_ID
        and d_cs.industry_business_code = d_pc_rels.INDUSTRY_BUSINESS_CDE
        and d_cbc.competency_business_cde = d_pc_rels.COMPETENCY_BUSINESS_CDE
      -- [SOLD-TO-CHAIN] Single join to module
      left join soldto_www_cte stw
        on stw.bukrs = f_act_rec_inv_cte.bukrs
        and stw.belnr = f_act_rec_inv_cte.belnr
        and stw.gjahr = f_act_rec_inv_cte.gjahr
        and stw.buzei = f_act_rec_inv_cte.buzei
  qualify
    row_number() over (
        partition by
          f_act_rec_inv_cte.bukrs,
          f_act_rec_inv_cte.kunnr,
          f_act_rec_inv_cte.belnr,
          f_act_rec_inv_cte.gjahr,
          f_act_rec_inv_cte.buzei,
          f_bfod.auzei
        order by
          d_ted_profit_center.eff_date desc,
          if(f_act_rec_inv_cte.item_status = 'CLEARED', 1, 2) asc
      ) = 1
)
SELECT
  b.company_code,
  b.customer_account_number,
  b.accounting_document_id,
  b.accounting_document_posting_fiscal_year_id,
  b.accounting_document_item_id,
  b.accounting_document_type_code,
  b.accounting_document_create_date,
  b.accounting_document_posting_date,
  b.accounting_document_item_clearing_document_id,
  b.accounting_document_item_clearing_date,
  b.accounting_document_item_credit_or_debit_code,
  b.accounting_document_item_posting_key_used_in_payment_indicator,
  b.accounting_document_item_invoice_document_id,
  b.accounting_document_item_text,
  b.accounting_document_item_baseline_date,
  b.accounting_document_item_due_date,
  b.accounting_document_item_payment_terms_code,
  b.accounting_document_item_cash_discount_days_quantity_1,
  b.accounting_document_item_cash_discount_days_quantity_2,
  b.accounting_document_create_network_user_id,
  b.accounting_document_item_document_currency_code,
  b.accounting_document_item_functional_currency_code,
  b.accounting_document_item_payment_reason_code,
  b.accounting_document_item_clearing_status_name,
  case
    when
      b.resolved_cbc is not null
      and (
        coalesce(b.initial_business_unit_id, '') in ('', 'LGL', 'TMP', 'CRP')
        or b.profit_center_id is null
      )
      and b.override_sap_profit_center is not null
    then
      b.override_sap_profit_center
    when
      b.resolved_cbc is null
      and coalesce(b.initial_business_unit_id, '') in ('LGL', 'TMP', 'CRP')
    then
      null
    else b.profit_center_id
  end as ted_overriding_profit_center_id,
  b.profit_center_id,
  b.ted_profit_center_id,
  b.ted_profit_center_effective_date,
  b.accounting_document_breakdown_item_id,
  b.accounting_document_item_document_currency_amount,
  b.accouting_document_item_general_ledger_reconciliation_account_number,
  b.accounting_document_item_functional_currency_amount,
  coalesce(
    finance_tbl.convert_amount_to_usd_currency(
      b.accounting_document_item_functional_currency_amount,
      b.exchange_rate,
      b.from_currency_ratio,
      b.target_currency_ratio
    ),
    b.accounting_document_item_functional_currency_amount
  ) accounting_document_item_actual_rate_functional_amount,
  b.accounting_document_item_tariff_percentage,
  admin.get_data_security_tag(
    array(
      coalesce(b.customer_owning_business_unit_dsg_id, 0),
      coalesce(b.sales_territory_owning_business_unit_dsg_id, 0),
      coalesce(b.gam_global_business_unit_owning_business_unit_dsg_id, 0),
      coalesce(b.company_dsg_id, 0),
      coalesce(d_bu_final.business_unit_dsg_id, b.business_unit_dsg_id, 0)
    )
  ) data_security_tag_id,
  coalesce(d_bu_final.business_unit_group_id, b.business_unit_group_id) business_unit_group_id,
  b.accounting_document_item_functional_currency_amount
    * b.currency_exchange_rate_to_USD_multiplier_factor accounting_document_item_budget_rate_amount,
  --- Debug columns ---
   b.sold_to_customer_number,
  --   b.sold_to_resolution_source,
  --   b.mapped_company_code,
  --   b.customer_key_resolution_tier,
  --   b.debug_soldto_from_vbrk,
  --   b.debug_soldto_from_vbfa_direct,
  --   b.debug_soldto_from_vbfa_delivery,
  --   b.debug_soldto_from_0048_bseg,
  --b.customer_key_id,
  b.sales_organization_id,
  b.distribution_channel_code,
  b.worldwide_customer_level_1_number,
  b.worldwide_customer_level_1_name,
  b.worldwide_customer_level_2_number,
  b.worldwide_customer_level_2_name,
  b.country_region_level_1_name,
  b.country_region_level_2_name,
  b.gam_level_3_assoc_full_name,
  b.account_manager_level_6_assoc_full_name,
  b.account_manager_level_7_assoc_full_name,
  b.sales_territory_level_6_assoc_full_name,
  b.sales_territory_level_7_assoc_full_name,
  b.sales_office_code,
  b.sales_office_name,
  b.account_manager_assoc_full_name,
  b.sales_territory_assoc_full_name,
  b.sensors_alternate_sales_territory_code,
  b.sensors_alternate_sales_territory_name
FROM
  ar_base b
    left join master_data_tbl.dim_profit_center d_pc_final
      on d_pc_final.profit_center_id
        = case
          when
            b.resolved_cbc is not null
            and (
              coalesce(b.initial_business_unit_id, '') in ('', 'LGL', 'TMP', 'CRP')
              or b.profit_center_id is null
            )
            and b.override_sap_profit_center is not null
          then
            b.override_sap_profit_center
          when
            b.resolved_cbc is null
            and coalesce(b.initial_business_unit_id, '') in ('LGL', 'TMP', 'CRP')
          then
            null
          else b.profit_center_id
        end
      and d_pc_final.record_active_indicator = 'Y'
    left join master_data_tbl.dim_business_unit d_bu_final
      on d_pc_final.business_unit_id = d_bu_final.business_unit_id
      and d_bu_final.record_active_indicator = 'Y'

-- COMMAND ----------

-- DBTITLE 1,Update L1 detail TRANSFORM_QUERY
update admin.data_flow_pb_detail set TRANSFORM_QUERY =
"
WITH f_accounts_receivable_invoice_cte AS (
  select
    bukrs,
    kunnr,
    belnr,
    gjahr,
    buzei,
    blart,
    bldat,
    budat,
    augbl,
    augdt,
    shkzg,
    xzahl,
    vbeln,
    sgtxt,
    zfbdt,
    zterm,
    zbd1t,
    zbd2t,
    zbd3t,
    waers,
    wrbtr,
    dmbtr,
    rstgr,
    saknr,
    'OPEN' item_status
  from
    finance_conf.sapPR2028__bsid
  union all
  select
    bukrs,
    kunnr,
    belnr,
    gjahr,
    buzei,
    blart,
    bldat,
    budat,
    augbl,
    augdt,
    shkzg,
    xzahl,
    vbeln,
    sgtxt,
    zfbdt,
    zterm,
    zbd1t,
    zbd2t,
    zbd3t,
    waers,
    wrbtr,
    dmbtr,
    rstgr,
    saknr,
    'CLEARED' item_status
  from
    finance_conf.sapPR2028__bsad
),
d_t052_distinct_cte AS (
  select
    zterm,
    max(try_cast(ztag1 as integer)) ztag1
  from
    master_data_conf.sapPR2028__t052
  group by
    all
),
d_material_cte AS (
  select
    vbeln,
    concat_ws(', ', collect_list(distinct matnr)) material_number
  from
    direct_sales_conf.sapPR2028__vbrp
  group by
    all
),
d_tariff_percentages_cte AS (
  select
    bseg.belnr belnr,
    bseg.bukrs bukrs,
    bseg.gjahr gjahr,
    max(vbrp.matnr) matnr
  from
    finance_conf.sapPR2028__bseg bseg
      left join direct_sales_conf.sapPR2028__vbrp vbrp
        on vbrp.vbeln = bseg.kidno
        and vbrp.matnr in ('1-120037-2')
        and vbrp.AUBEL = bseg.VBEL2
        and vbrp.AUPOS = bseg.POSN2
  group by
    all
),
d_tariff_konv_cte AS (
  select
    knumv,
    max(kbetr) kbetr
  from
    pricing_l0_raw.sapPR2028__konv
  where
    kschl in ('ZTRF', 'ZTRM')
  group by
    all
),
ref_gbl_product AS (
  select
    PROD_CODE,
    PROD_BUSLN_FNCTN_ID
  from
    master_data_conf.gbl_current__gbl_product
  where
    coalesce(PROD_BUSLN_FNCTN_ID, '') != ''
),
ref_cbc_by_billing_doc AS (
  select
    vbrp.vbeln,
    gp.PROD_BUSLN_FNCTN_ID as competency_business_cde,
    row_number() over (partition by vbrp.vbeln order by abs(vbrp.netwr) desc) as rn
  from
    direct_sales_conf.sapPR2028__vbrp vbrp
      inner join master_data_l0_raw.sapPR2028__mara mara
        on vbrp.matnr = mara.matnr
      inner join ref_gbl_product gp
        on mara.prdha = gp.PROD_CODE
),
sd_soldto_direct_cte AS (
  select
    bukrs,
    belnr,
    gjahr,
    buzei,
    soldto_kunnr_from_sd_direct
  from
    (
      select
        ar.bukrs,
        ar.belnr,
        ar.gjahr,
        ar.buzei,
        vbak.kunnr as soldto_kunnr_from_sd_direct,
        row_number() over (
            partition by ar.bukrs, ar.belnr, ar.gjahr, ar.buzei
            order by vbfa.vbelv
          ) as rn
      from
        f_accounts_receivable_invoice_cte ar
          inner join direct_sales_l0_raw.sapPR2028__vbfa vbfa
            on vbfa.vbeln = ar.vbeln
            and vbfa.vbtyp_v in ('C', 'H', 'B', 'E', 'F', 'G', 'K', 'L', 'I')
          inner join direct_sales_l0_raw.sapPR2028__vbak vbak
            on vbak.vbeln = vbfa.vbelv
            and vbak.bukrs_vf
              = case
                when ar.bukrs = '1082' then '0048'
                else ar.bukrs
              end
      where
        coalesce(trim(ar.vbeln), '') <> ''
    )
  where
    rn = 1
),
sd_soldto_delivery_fallback_cte AS (
  select
    bukrs,
    belnr,
    gjahr,
    buzei,
    soldto_kunnr_from_sd_fallback
  from
    (
      select
        ar.bukrs,
        ar.belnr,
        ar.gjahr,
        ar.buzei,
        vbak.kunnr as soldto_kunnr_from_sd_fallback,
        row_number() over (
            partition by ar.bukrs, ar.belnr, ar.gjahr, ar.buzei
            order by
              case
                when vbfa_order.vbtyp_v = 'E' then 1
                when vbfa_order.vbtyp_v = 'C' then 2
                else 9
              end,
              vbfa_order.vbelv
          ) as rn
      from
        f_accounts_receivable_invoice_cte ar
          inner join direct_sales_l0_raw.sapPR2028__vbfa vbfa_delivery
            on vbfa_delivery.vbeln = ar.vbeln
            and vbfa_delivery.vbtyp_v = 'J'
            and coalesce(vbfa_delivery.vbtyp_n, '') <> 'X'
          inner join direct_sales_l0_raw.sapPR2028__vbfa vbfa_order
            on vbfa_order.vbeln = vbfa_delivery.vbelv
            and vbfa_order.vbtyp_v in ('E', 'C')
          inner join direct_sales_l0_raw.sapPR2028__vbak vbak
            on vbak.vbeln = vbfa_order.vbelv
      where
        coalesce(trim(ar.vbeln), '') <> ''
    )
  where
    rn = 1
),
bseg_customer_for_0048_cte AS (
  select
    bukrs,
    belnr,
    gjahr,
    bseg_customer_id
  from
    (
      select
        bukrs,
        belnr,
        gjahr,
        kunnr as bseg_customer_id,
        row_number() over (partition by bukrs, belnr, gjahr order by buzei) as rn
      from
        finance_conf.sapPR2028__bseg
      where
        koart = 'D'
        and coalesce(trim(kunnr), '') <> ''
        and bukrs = '0048'
    )
  where
    rn = 1
),
ref_customer_any_bukrs_cte AS (
  select
    submitted_customer_account_number as account_number,
    customer_key_id
  from
    master_data_l1_curated.dim_customer_current
  where
    source_system_id = 1
  qualify
    row_number() over (
        partition by submitted_customer_account_number
        order by customer_company_code asc, distribution_channel_code asc
      ) = 1
),
soldto_resolution_cte AS (
  select
    ar.bukrs,
    ar.belnr,
    ar.gjahr,
    ar.buzei,
    case
      when
        ar.bukrs = '0048'
        and bseg_0048.bseg_customer_id is not null
      then
        bseg_0048.bseg_customer_id
      else
        coalesce(
          nullif(trim(vbrk.kunag), ''),
          sd_direct.soldto_kunnr_from_sd_direct,
          sd_fb.soldto_kunnr_from_sd_fallback,
          ar.kunnr
        )
    end as sold_to_customer_number,
    case
      when
        ar.bukrs = '0048'
        and bseg_0048.bseg_customer_id is not null
      then
        'ABAP_003ANL_BSEG_0048_REFRESH'
      when nullif(trim(vbrk.kunag), '') is not null then 'BSEG_KIDNO_TO_VBRK_KUNAG'
      when sd_direct.soldto_kunnr_from_sd_direct is not null then 'VBFA_VBAK_DIRECT'
      when sd_fb.soldto_kunnr_from_sd_fallback is not null then 'VBFA_VBAK_DELIVERY_FALLBACK'
      else 'ORIGINAL_BSID_BSAD_KUNNR'
    end as sold_to_resolution_source,
    case
      when ar.bukrs = '0048' then '1082'
      else ar.bukrs
    end as mapped_company_code,
    case
      when cust_lookup_primary.customer_key_id is not null then 'PRIMARY_BUKRS'
      when cust_lookup_0048_fallback.customer_key_id is not null then 'FALLBACK_0048_TO_1082'
      when any_bukrs.customer_key_id is not null then 'FALLBACK_ANY_BUKRS'
      else 'UNRESOLVED'
    end as customer_key_resolution_tier,
    vbrk.kunag as debug_soldto_from_vbrk,
    sd_direct.soldto_kunnr_from_sd_direct as debug_soldto_from_vbfa_direct,
    sd_fb.soldto_kunnr_from_sd_fallback as debug_soldto_from_vbfa_delivery,
    bseg_0048.bseg_customer_id as debug_soldto_from_0048_bseg,
    coalesce(
      cust_lookup_primary.customer_key_id,
      cust_lookup_0048_fallback.customer_key_id,
      any_bukrs.customer_key_id
    ) as resolved_customer_key_id
  from
    f_accounts_receivable_invoice_cte ar
      left join finance_conf.sapPR2028__bseg f_bseg_stw
        on ar.belnr = f_bseg_stw.belnr
        and ar.bukrs = f_bseg_stw.bukrs
        and ar.gjahr = f_bseg_stw.gjahr
        and ltrim('0', ar.buzei) = ltrim('0', f_bseg_stw.buzei)
      left join direct_sales_raw.sapPR2028__vbrk vbrk
        on vbrk.vbeln = f_bseg_stw.kidno
      left join sd_soldto_direct_cte sd_direct
        on sd_direct.bukrs = ar.bukrs
        and sd_direct.belnr = ar.belnr
        and sd_direct.gjahr = ar.gjahr
        and sd_direct.buzei = ar.buzei
      left join sd_soldto_delivery_fallback_cte sd_fb
        on sd_fb.bukrs = ar.bukrs
        and sd_fb.belnr = ar.belnr
        and sd_fb.gjahr = ar.gjahr
        and sd_fb.buzei = ar.buzei
      left join bseg_customer_for_0048_cte bseg_0048
        on bseg_0048.bukrs = ar.bukrs
        and bseg_0048.belnr = ar.belnr
        and bseg_0048.gjahr = ar.gjahr
      LEFT JOIN LATERAL (
        SELECT
          customer_key_id
        FROM
          master_data_l1_curated.dim_customer_current
        WHERE
          submitted_customer_account_number
            = case
              when
                ar.bukrs = '0048'
                and bseg_0048.bseg_customer_id is not null
              then
                bseg_0048.bseg_customer_id
              else
                coalesce(
                  nullif(trim(vbrk.kunag), ''),
                  sd_direct.soldto_kunnr_from_sd_direct,
                  sd_fb.soldto_kunnr_from_sd_fallback,
                  ar.kunnr
                )
            end
          AND customer_company_code = ar.bukrs
          AND source_system_id = 1
        ORDER BY
          distribution_channel_code ASC
        LIMIT 1
      ) cust_lookup_primary
      LEFT JOIN LATERAL (
        SELECT
          customer_key_id
        FROM
          master_data_l1_curated.dim_customer_current
        WHERE
          submitted_customer_account_number
            = case
              when
                ar.bukrs = '0048'
                and bseg_0048.bseg_customer_id is not null
              then
                bseg_0048.bseg_customer_id
              else
                coalesce(
                  nullif(trim(vbrk.kunag), ''),
                  sd_direct.soldto_kunnr_from_sd_direct,
                  sd_fb.soldto_kunnr_from_sd_fallback,
                  ar.kunnr
                )
            end
          AND customer_company_code = '1082'
          AND source_system_id = 1
          AND ar.bukrs = '0048'
        ORDER BY
          distribution_channel_code ASC
        LIMIT 1
      ) cust_lookup_0048_fallback
      left join ref_customer_any_bukrs_cte any_bukrs
        on any_bukrs.account_number
          = case
            when
              ar.bukrs = '0048'
              and bseg_0048.bseg_customer_id is not null
            then
              bseg_0048.bseg_customer_id
            else
              coalesce(
                nullif(trim(vbrk.kunag), ''),
                sd_direct.soldto_kunnr_from_sd_direct,
                sd_fb.soldto_kunnr_from_sd_fallback,
                ar.kunnr
              )
          end
),
soldto_www_cte AS (
  select
    sr.bukrs,
    sr.belnr,
    sr.gjahr,
    sr.buzei,
    sr.sold_to_customer_number,
    sr.sold_to_resolution_source,
    sr.mapped_company_code,
    sr.customer_key_resolution_tier,
    sr.debug_soldto_from_vbrk,
    sr.debug_soldto_from_vbfa_direct,
    sr.debug_soldto_from_vbfa_delivery,
    sr.debug_soldto_from_0048_bseg,
    d_csa.customer_key_id as soldto_customer_key_id,
    d_csa.sales_organization_id,
    d_csa.distribution_channel_code,
    d_csa.worldwide_customer_level_1_number,
    d_csa.worldwide_customer_level_1_label as worldwide_customer_level_1_name,
    d_csa.worldwide_customer_level_2_number,
    d_csa.worldwide_customer_level_2_label as worldwide_customer_level_2_name,
    d_csa.country_region_level_1_label as country_region_level_1_name,
    d_csa.country_region_level_2_label as country_region_level_2_name,
    d_csa.gam_level_3_assoc_full_name,
    d_csa.account_manager_level_1_assoc_full_name as account_manager_assoc_full_name,
    d_csa.account_manager_level_6_assoc_full_name,
    d_csa.account_manager_level_7_assoc_full_name,
    d_csa.sales_territory_level_1_assoc_full_name as sales_territory_assoc_full_name,
    d_csa.sales_territory_level_6_assoc_full_name,
    d_csa.sales_territory_level_7_assoc_full_name,
    d_csa.sales_office_code,
    d_csa.sales_office_label as sales_office_name
  from
    soldto_resolution_cte sr
      left join master_data_l1_curated.dim_customer_current d_csa
        on d_csa.customer_key_id = sr.resolved_customer_key_id
),
ar_base AS (
  select
    f_act_rec_inv_cte.bukrs company_code,
    f_act_rec_inv_cte.kunnr customer_account_number,
    f_act_rec_inv_cte.belnr accounting_document_id,
    f_act_rec_inv_cte.gjahr accounting_document_posting_fiscal_year_id,
    f_act_rec_inv_cte.buzei accounting_document_item_id,
    f_act_rec_inv_cte.blart accounting_document_type_code,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.bldat
    ) accounting_document_create_date,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.budat
    ) accounting_document_posting_date,
    f_act_rec_inv_cte.augbl accounting_document_item_clearing_document_id,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.augdt
    ) accounting_document_item_clearing_date,
    f_act_rec_inv_cte.shkzg accounting_document_item_credit_or_debit_code,
    f_act_rec_inv_cte.xzahl accounting_document_item_posting_key_used_in_payment_indicator,
    f_act_rec_inv_cte.vbeln accounting_document_item_invoice_document_id,
    f_act_rec_inv_cte.sgtxt accounting_document_item_text,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.zfbdt
    ) accounting_document_item_baseline_date,
    date_add(
      finance_tbl.convert_sap_datestring_to_date(
        if(
          coalesce(f_act_rec_inv_cte.zfbdt, '') in ('', '00000000'),
          f_act_rec_inv_cte.bldat,
          f_act_rec_inv_cte.zfbdt
        )
      ),
      case
        when
          f_act_rec_inv_cte.shkzg = 'H'
          and coalesce(f_bseg.rebzg, '') = ''
        then
          0
        else
          coalesce(
            nullif(try_cast(f_act_rec_inv_cte.zbd3t as integer), 0),
            nullif(try_cast(f_act_rec_inv_cte.zbd2t as integer), 0),
            nullif(try_cast(f_act_rec_inv_cte.zbd1t as integer), 0),
            d_t052_cte.ztag1,
            0
          )
      end
    ) accounting_document_item_due_date,
    f_act_rec_inv_cte.zterm accounting_document_item_payment_terms_code,
    f_act_rec_inv_cte.zbd1t accounting_document_item_cash_discount_days_quantity_1,
    f_act_rec_inv_cte.zbd2t accounting_document_item_cash_discount_days_quantity_2,
    f_bkpf.usnam accounting_document_create_network_user_id,
    f_act_rec_inv_cte.waers accounting_document_item_document_currency_code,
    f_t001.waers accounting_document_item_functional_currency_code,
    f_act_rec_inv_cte.rstgr accounting_document_item_payment_reason_code,
    f_act_rec_inv_cte.item_status accounting_document_item_clearing_status_name,
    coalesce(f_bfod.prctr, f_bseg.prctr) as profit_center_id,
    d_ted_profit_center.ztedprc ted_profit_center_id,
    finance_tbl.convert_sap_datestring_to_date(
      d_ted_profit_center.eff_date
    ) ted_profit_center_effective_date,
    f_bfod.auzei accounting_document_breakdown_item_id,
    if(
      coalesce(f_bfod.auzei, 1) = 1,
      finance_tbl.round_and_set_signal_amount(f_act_rec_inv_cte.wrbtr, f_act_rec_inv_cte.shkzg),
      0
    ) accounting_document_item_document_currency_amount,
    f_act_rec_inv_cte.saknr accouting_document_item_general_ledger_reconciliation_account_number,
    round(
      coalesce(
        finance_tbl.round_and_set_signal_amount(f_bfod.dmbtr, f_bfod.shkzg),
        finance_tbl.round_and_set_signal_amount(f_act_rec_inv_cte.dmbtr, f_act_rec_inv_cte.shkzg)
      )
        * power(10, 2 - coalesce(f_tcurx_functional.currdec, 2)),
      3
    ) accounting_document_item_functional_currency_amount,
    case
      when
        d_tk_cte.kbetr is not null
      then
        coalesce(d_tk_cte.kbetr / 10, if(d_tp_cte.matnr is not null, 100, 0))
      when d_tp_cte.matnr is not null then 100
      else 0
    end accounting_document_item_tariff_percentage,
    d_cs.customer_owning_business_unit_dsg_id,
    d_cs.sales_territory_owning_business_unit_dsg_id,
    d_cs.gam_global_business_unit_owning_business_unit_dsg_id,
    d_dc.company_dsg_id,
    d_cs.industry_business_code,
    f_t001.waers as company_currency_code,
    d_cer.currency_exchange_rate_to_USD_multiplier_factor,
    d_curr.exchange_rate,
    d_curr.from_currency_ratio,
    d_curr.target_currency_ratio,
    d_cbc.competency_business_cde as resolved_cbc,
    d_pc_rels.MGE_PROFIT_CENTER_ABBREV_ID as override_bu_abbrev,
    d_pc_rels.SAP_PROFIT_CENTER_CDE as override_sap_profit_center,
    d_bu.business_unit_id as initial_business_unit_id,
    d_bu.business_unit_dsg_id,
    d_bu.business_unit_group_id,
    stw.sold_to_customer_number,
    stw.sold_to_resolution_source,
    stw.mapped_company_code,
    stw.customer_key_resolution_tier,
    stw.debug_soldto_from_vbrk,
    stw.debug_soldto_from_vbfa_direct,
    stw.debug_soldto_from_vbfa_delivery,
    stw.debug_soldto_from_0048_bseg,
    stw.soldto_customer_key_id as customer_key_id,
    stw.sales_organization_id,
    stw.distribution_channel_code,
    stw.worldwide_customer_level_1_number,
    stw.worldwide_customer_level_1_name,
    stw.worldwide_customer_level_2_number,
    stw.worldwide_customer_level_2_name,
    stw.country_region_level_1_name,
    stw.country_region_level_2_name,
    stw.gam_level_3_assoc_full_name,
    stw.account_manager_level_6_assoc_full_name,
    stw.account_manager_level_7_assoc_full_name,
    stw.sales_territory_level_6_assoc_full_name,
    stw.sales_territory_level_7_assoc_full_name,
    stw.sales_office_code,
    stw.sales_office_name,
    stw.account_manager_assoc_full_name,
    stw.sales_territory_assoc_full_name,
    d_cs.sensors_alternate_sales_territory_code,
    d_cs.sensors_alternate_sales_territory_name
  from
    f_accounts_receivable_invoice_cte f_act_rec_inv_cte
      left join finance_conf.sapPR2028__bseg f_bseg
        on f_act_rec_inv_cte.belnr = f_bseg.belnr
        and ltrim('0', f_act_rec_inv_cte.buzei) = ltrim('0', f_bseg.buzei)
        and f_act_rec_inv_cte.bukrs = f_bseg.bukrs
        and f_act_rec_inv_cte.gjahr = f_bseg.gjahr
      left join finance_conf.sapPR2028__bkpf f_bkpf
        on f_act_rec_inv_cte.bukrs = f_bkpf.bukrs
        and f_act_rec_inv_cte.belnr = f_bkpf.belnr
        and f_act_rec_inv_cte.gjahr = f_bkpf.gjahr
      left join master_data_conf.sapPR2028__t001 f_t001
        on f_act_rec_inv_cte.bukrs = f_t001.bukrs
      left join finance_l0_raw.sapPR2028__tcurx f_tcurx_functional
        on f_tcurx_functional.currkey = f_t001.waers
      left join finance_l0_raw.sapPR2028__tcurx f_tcurx_document
        on f_tcurx_document.currkey = f_act_rec_inv_cte.waers
      left join finance_tbl.dim_currencies_dates_rates d_curr
        on d_curr.reference_date
          = finance_tbl.convert_sap_datestring_to_date(
            coalesce(nullif(f_act_rec_inv_cte.augdt, 0), f_act_rec_inv_cte.budat)
          )
        and d_curr.from_currency = f_t001.waers
      left join d_t052_distinct_cte d_t052_cte
        on f_act_rec_inv_cte.zterm = d_t052_cte.zterm
      left join direct_sales_raw.sapPR2028__vbrk vbrk
        on vbrk.vbeln = f_bseg.kidno
      left join d_tariff_konv_cte d_tk_cte
        on d_tk_cte.knumv = vbrk.knumv
      left join d_tariff_percentages_cte d_tp_cte
        on f_bkpf.belnr = d_tp_cte.belnr
        and f_bkpf.bukrs = d_tp_cte.bukrs
        and f_bkpf.gjahr = d_tp_cte.gjahr
      left join master_data_tbl.dim_customer d_cs
        on d_cs.customer_account_number = regexp_replace(f_act_rec_inv_cte.kunnr, '^0+', '')
        and d_cs.customer_company_code = f_act_rec_inv_cte.bukrs
        and d_cs.source_system_id = 1
        and d_cs.customer_company_legacy_format_account_number_rank = 1
        and d_cs.record_active_indicator = 'Y'
      left join master_data_tbl.dim_company d_dc
        on d_dc.company_code = f_act_rec_inv_cte.bukrs
        and d_dc.record_active_indicator = 'Y'
      left join finance_l0_raw.sapPR2028__bfod_a f_bfod
        on f_bfod.belnr = f_act_rec_inv_cte.belnr
        and f_bfod.buzei = f_act_rec_inv_cte.buzei
        and f_bfod.gjahr = f_act_rec_inv_cte.gjahr
        and f_bfod.bukrs = f_act_rec_inv_cte.bukrs
      left join master_data_tbl.dim_profit_center d_pc
        on d_pc.profit_center_id = coalesce(f_bfod.prctr, f_bseg.prctr)
        and d_pc.record_active_indicator = 'Y'
      left join master_data_tbl.dim_business_unit d_bu
        on d_pc.business_unit_id = d_bu.business_unit_id
        and d_bu.record_active_indicator = 'Y'
      left join master_data_tbl.dim_currency_exchange_rate d_cer
        on d_cer.currency_code = f_t001.waers
        and d_cer.currency_exchange_rate_type_code = 1
        and to_date(FROM_UNIXTIME(UNIX_TIMESTAMP())) between
          to_date(d_cer.effective_from_date)
        and
          to_date(d_cer.effective_to_date)
      left join finance_l0_raw.sapPR2028__zpcafence d_ted_profit_center
        on d_ted_profit_center.prctr = coalesce(f_bfod.prctr, f_bseg.prctr)
        and d_ted_profit_center.eff_date <= f_act_rec_inv_cte.budat
      left join ref_cbc_by_billing_doc d_cbc
        on f_bseg.kidno = d_cbc.vbeln
        and d_cbc.rn = 1
        and coalesce(f_bseg.kidno, '') != ''
      left join master_data_conf.gbl_current__gbl_mge_profit_center_rels d_pc_rels
        on f_act_rec_inv_cte.bukrs = d_pc_rels.ORGANIZATION_ID
        and d_cs.industry_business_code = d_pc_rels.INDUSTRY_BUSINESS_CDE
        and d_cbc.competency_business_cde = d_pc_rels.COMPETENCY_BUSINESS_CDE
      left join soldto_www_cte stw
        on stw.bukrs = f_act_rec_inv_cte.bukrs
        and stw.belnr = f_act_rec_inv_cte.belnr
        and stw.gjahr = f_act_rec_inv_cte.gjahr
        and stw.buzei = f_act_rec_inv_cte.buzei
  qualify
    row_number() over (
        partition by
          f_act_rec_inv_cte.bukrs,
          f_act_rec_inv_cte.kunnr,
          f_act_rec_inv_cte.belnr,
          f_act_rec_inv_cte.gjahr,
          f_act_rec_inv_cte.buzei,
          f_bfod.auzei
        order by
          d_ted_profit_center.eff_date desc,
          if(f_act_rec_inv_cte.item_status = 'CLEARED', 1, 2) asc
      ) = 1
)
SELECT
  b.company_code,
  b.customer_account_number,
  b.accounting_document_id,
  b.accounting_document_posting_fiscal_year_id,
  b.accounting_document_item_id,
  b.accounting_document_type_code,
  b.accounting_document_create_date,
  b.accounting_document_posting_date,
  b.accounting_document_item_clearing_document_id,
  b.accounting_document_item_clearing_date,
  b.accounting_document_item_credit_or_debit_code,
  b.accounting_document_item_posting_key_used_in_payment_indicator,
  b.accounting_document_item_invoice_document_id,
  b.accounting_document_item_text,
  b.accounting_document_item_baseline_date,
  b.accounting_document_item_due_date,
  b.accounting_document_item_payment_terms_code,
  b.accounting_document_item_cash_discount_days_quantity_1,
  b.accounting_document_item_cash_discount_days_quantity_2,
  b.accounting_document_create_network_user_id,
  b.accounting_document_item_document_currency_code,
  b.accounting_document_item_functional_currency_code,
  b.accounting_document_item_payment_reason_code,
  b.accounting_document_item_clearing_status_name,
  case
    when
      b.resolved_cbc is not null
      and (
        coalesce(b.initial_business_unit_id, '') in ('', 'LGL', 'TMP', 'CRP')
        or b.profit_center_id is null
      )
      and b.override_sap_profit_center is not null
    then
      b.override_sap_profit_center
    when
      b.resolved_cbc is null
      and coalesce(b.initial_business_unit_id, '') in ('LGL', 'TMP', 'CRP')
    then
      null
    else b.profit_center_id
  end as ted_overriding_profit_center_id,
  b.profit_center_id,
  b.ted_profit_center_id,
  b.ted_profit_center_effective_date,
  b.accounting_document_breakdown_item_id,
  b.accounting_document_item_document_currency_amount,
  b.accouting_document_item_general_ledger_reconciliation_account_number,
  b.accounting_document_item_functional_currency_amount,
  coalesce(
    finance_tbl.convert_amount_to_usd_currency(
      b.accounting_document_item_functional_currency_amount,
      b.exchange_rate,
      b.from_currency_ratio,
      b.target_currency_ratio
    ),
    b.accounting_document_item_functional_currency_amount
  ) accounting_document_item_actual_rate_functional_amount,
  b.accounting_document_item_tariff_percentage,
  admin.get_data_security_tag(
    array(
      coalesce(b.customer_owning_business_unit_dsg_id, 0),
      coalesce(b.sales_territory_owning_business_unit_dsg_id, 0),
      coalesce(b.gam_global_business_unit_owning_business_unit_dsg_id, 0),
      coalesce(b.company_dsg_id, 0),
      coalesce(d_bu_final.business_unit_dsg_id, b.business_unit_dsg_id, 0)
    )
  ) data_security_tag_id,
  coalesce(d_bu_final.business_unit_group_id, b.business_unit_group_id) business_unit_group_id,
  b.accounting_document_item_functional_currency_amount
    * b.currency_exchange_rate_to_USD_multiplier_factor accounting_document_item_budget_rate_amount,
  b.sold_to_customer_number,
  b.sales_organization_id,
  b.distribution_channel_code,
  b.worldwide_customer_level_1_number,
  b.worldwide_customer_level_1_name,
  b.worldwide_customer_level_2_number,
  b.worldwide_customer_level_2_name,
  b.country_region_level_1_name,
  b.country_region_level_2_name,
  b.gam_level_3_assoc_full_name,
  b.account_manager_level_6_assoc_full_name,
  b.account_manager_level_7_assoc_full_name,
  b.sales_territory_level_6_assoc_full_name,
  b.sales_territory_level_7_assoc_full_name,
  b.sales_office_code,
  b.sales_office_name,
  b.account_manager_assoc_full_name,
  b.sales_territory_assoc_full_name,
  b.sensors_alternate_sales_territory_code,
  b.sensors_alternate_sales_territory_name
FROM
  ar_base b
    left join master_data_tbl.dim_profit_center d_pc_final
      on d_pc_final.profit_center_id
        = case
          when
            b.resolved_cbc is not null
            and (
              coalesce(b.initial_business_unit_id, '') in ('', 'LGL', 'TMP', 'CRP')
              or b.profit_center_id is null
            )
            and b.override_sap_profit_center is not null
          then
            b.override_sap_profit_center
          when
            b.resolved_cbc is null
            and coalesce(b.initial_business_unit_id, '') in ('LGL', 'TMP', 'CRP')
          then
            null
          else b.profit_center_id
        end
      and d_pc_final.record_active_indicator = 'Y'
    left join master_data_tbl.dim_business_unit d_bu_final
      on d_pc_final.business_unit_id = d_bu_final.business_unit_id
      and d_bu_final.record_active_indicator = 'Y'
"
where DATA_FLOW_GROUP_ID = "FINANCE_FIN360_ACCOUNTS_RECEIVABLE_L1"
and TARGET_OBJ_NAME = "fact_accounts_receivable_invoice";
