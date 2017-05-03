# coding: utf-8

from niamoto.db import metadata as niamoto_db_meta


def make_taxon_tree(tree, parent_id=None, last_id=0):
    if isinstance(tree, int):
        current_id = last_id + 1
        data = []
        for i in range(tree):
            data.append({
                'id': current_id + i,
                'full_name': 'taxon_{}'.format(current_id + i),
                'rank_name': 'taxon_{}'.format(current_id + i),
                'rank': niamoto_db_meta.TaxonRankEnum.FAMILIA,
                'parent_id': parent_id,
                'synonyms': {},
                'mptt_left': 0,
                'mptt_right': 0,
                'mptt_tree_id': 0,
                'mptt_depth': 0,
            })
        return data, current_id + tree - 1
    data = []
    current_id = last_id
    for i in range(len(tree)):
        current_id += 1
        data.append({
            'id': current_id,
            'full_name': 'taxon_{}'.format(current_id),
            'rank_name': 'taxon_{}'.format(current_id),
            'rank': niamoto_db_meta.TaxonRankEnum.FAMILIA,
            'parent_id': parent_id,
            'synonyms': {},
            'mptt_left': 0,
            'mptt_right': 0,
            'mptt_tree_id': 0,
            'mptt_depth': 0,
        })
        k = current_id
        t = tree[i]
        if isinstance(t, int):
            st, current_id = make_taxon_tree(
                t, current_id,
                last_id=current_id
            )
            data += st
        else:
            for j in t:
                st, current_id = make_taxon_tree(
                    j, k,
                    last_id=current_id
                )
                data += st
    return data, current_id
