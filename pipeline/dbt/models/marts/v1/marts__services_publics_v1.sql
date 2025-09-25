{{ unnest(
    from=ref('marts__services_v1'),
    column='publics',
    foreign_key='id',
    fk_alias='service_id'
) }}
