{{ unnest(
    from=ref('marts__services'),
    column='modes_accueil',
    foreign_key='id',
    fk_alias='service_id'
) }}
