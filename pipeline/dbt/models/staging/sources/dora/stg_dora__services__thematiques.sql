{{ unnest(
    from=ref('stg_dora__services'),
    column='thematiques',
    foreign_key='id',
    fk_alias='service_id'
) }}
