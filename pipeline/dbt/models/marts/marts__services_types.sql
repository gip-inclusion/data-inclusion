{{ unnest(
    from=ref('marts__services'),
    column='types',
    foreign_key='_di_surrogate_id',
    fk_alias='service_surrogate_id'
) }}
