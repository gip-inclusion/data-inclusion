{{ unnest(
    from=ref('marts__structures_v1'),
    column='reseaux_porteurs',
    foreign_key='id',
    fk_alias='structure_id'
) }}
