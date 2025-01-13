{{
    config(
    materialized = 'incremental',
    unique_key = ['listing_id','reviewer_name'],
    merge_update_columns = ['review_text','REVIEW_DATE']
    )
}}
SELECT * FROM {{ ref('src_reviews') }}
WHERE review_text is not null
{% if is_incremental() %}
and review_date > (select max(review_date) from {{this}})
{% endif %}