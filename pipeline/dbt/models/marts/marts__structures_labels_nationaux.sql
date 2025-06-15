{{ unnest(
    from=ref('marts__structures'),
    column='labels_nationaux',
    foreign_key='_di_surrogate_id',
    fk_alias='structure_surrogate_id'
) }}
