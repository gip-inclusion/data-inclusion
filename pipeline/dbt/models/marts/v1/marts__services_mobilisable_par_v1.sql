{{ unnest(
    from=ref('marts__services_v1'),
    column='mobilisable_par',
    foreign_key='id',
    fk_alias='service_id'
) }}
