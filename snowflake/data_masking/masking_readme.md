# Documentation of data masking in dbt
Theese macros can be used to create Snowflake masking policies using dbt. This documentation explains the following:

- How to create and apply
- How to unapply and drop

To find more out about Snowflake's Dynamic Data Masking, see it's documentation [here](https://docs.snowflake.com/en/user-guide/security-column-ddm-use.html#).

## Preview Installation
TODO

## How to create and apply data masking in dbt using these macros

1. Create a model that defines the masking policy using the 'masking_policy' materialization. For example 'models/custom/masking_policies/mask_customer_gender.sql':
```
{{
    config(
        materialized='masking_policy'
    )
}}

(val string) returns string ->
    case
      when exists (
        select role from {{ ref('s_mask_entitlement_customer') }}
        where
          current_role() ilike role and
          gender
        ) then val
      else '********'
    end
```
2. Maintain this masking policy in the YAML file of the model. 'masking_policy' is the [meta_key](https://docs.getdbt.com/reference/resource-configs/meta) used inside the macros. 'mask_customer_gender' is the name of the masking policy:
```
models:
  - name: md_customer_restricted
    columns:
      - name: gender
        description: ""
        meta:
          masking_policy: mask_customer_gender
```
3. Maintain the masking policy as a [post_hook](https://docs.getdbt.com/reference/resource-configs/pre-hook-post-hook) of the model to be masked. The mp parameter is the name of the masking_policy. Its possible to provide a list.
```
post_hook="{{ apply_masking_policy(mp='mask_customer_gender') }}"
```
or as a list
``` 
post_hook=["{{sample_macro1()}}","{{ apply_masking_policy(['mask_customer_gender','mask_customer_personaltitle']) }}"]
```
4. Run/build as usual. Here
```
dbt build --select +md_customer_restricted
```
will do the following in this order
  1. create the respective masking policies
  2. create/load the md_customer_restricted model
  3. apply the masking policies on the columns of the md_customer_restricted model 

## Unapply and drop masking policies

> **_NOTE:_** Masking policies can only be dropped, if its not applied to any table. Also the masking policies are created using 'create if not exists'. If you change in existing one, you need to unapply/drop before as well.

To unapply and drop (all) masking policies the macro 'drop_masking_policies' can be used as a [run-operation](https://docs.getdbt.com/reference/commands/run-operation). Its possible to define the following optional parameters:
```
meta_key:    Optional to define the name of the meta_key in the yaml files.
drop_mp:     Optional to disable the drop of the masking policies and only unapply them on the columns.
mp_schema:   Optional to change the schema, where the masking policies are stored.
mp_name:     Optional to select a single masking_policy to drop/unapply.
```
