{{ unnest(
    from=ref('marts__services'),
    column='thematiques',
    foreign_key='id',
    fk_alias='service_id'
) }}
