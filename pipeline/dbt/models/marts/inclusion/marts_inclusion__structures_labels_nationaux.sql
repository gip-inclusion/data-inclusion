{{ unnest(
    from=ref('marts_inclusion__structures'),
    column='labels_nationaux',
    foreign_key='_di_surrogate_id',
    fk_alias='structure_surrogate_id'
) }}
