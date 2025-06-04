{{ unnest(
    from=ref('marts_inclusion__services_v0'),
    column='pre_requis',
    foreign_key='_di_surrogate_id',
    fk_alias='service_surrogate_id'
) }}
