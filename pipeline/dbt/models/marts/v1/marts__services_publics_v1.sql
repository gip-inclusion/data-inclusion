{{ unnest(
    from=ref('marts__services'),
    column='publics',
    foreign_key='id',
    fk_alias='service_id'
) }}
