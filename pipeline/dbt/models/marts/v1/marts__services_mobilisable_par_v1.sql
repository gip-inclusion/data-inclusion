{{ unnest(
    from=ref('marts__services'),
    column='mobilisable_par',
    foreign_key='id',
    fk_alias='service_id'
) }}
