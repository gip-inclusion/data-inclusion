{{ unnest(
    from=ref('stg_dora__structures'),
    column='reseaux_porteurs',
    foreign_key='id',
    fk_alias='structure_id'
) }}
