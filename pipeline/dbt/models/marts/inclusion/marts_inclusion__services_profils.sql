{{ unnest(
    from=ref('marts_inclusion__services'),
    column='profils',
    foreign_key='_di_surrogate_id',
    fk_alias='service_surrogate_id'
) }}
