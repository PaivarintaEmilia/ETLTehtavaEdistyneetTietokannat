from sqlalchemy import text

from db import get_db


# Resepti-taulusta oltp-tietokannasta (id, created at ja user_id)
def _get_recipe_for_fact(_db):
    _query_str = "SELECT id, created_at, user_id FROM recipe"
    _query = text(_query_str)
    rows = _db.execute(_query)
    return rows.mappings().all()


# Haetaan kaikki recipe_dim-taulusta, koska sieltä tarvitaan key
def _get_recipe_dims(_dw):
    _query_str = "SELECT * FROM recipe_dim"
    _query = text(_query_str)
    rows = _dw.execute(_query)
    return rows.mappings().all()

# Haetaan tietoja user_dim-taulusta, koska sieltä tarvitaan key --> facta ja user_id --> voidaan hakea oikea tieto mikä key on
def _get_user_dims(_dw):
    _query_str = "SELECT user_key, user_id FROM user_dim"
    _query = text(_query_str)
    rows = _dw.execute(_query)
    return rows.mappings().all()


# Haetaan kaikki date_dim-taulusta
def _get_date_dims(_dw):
    _query_str = "SELECT * FROM date_dim"
    _query = text(_query_str)
    rows = _dw.execute(_query)
    return rows.mappings().all()



# Tarvitaan myös kaikista dim-tauluista keyt. Tällä vissiin haetaan vain date.key?
def _get_date_key(oltp_item, dates, key = 'created_at'):
    date = oltp_item[key]
    for d_dim in dates:
        if date.year == d_dim['year'] and date.month == d_dim['month'] and date.isocalendar().week == d_dim['week'
        ] and date.minute == d_dim['minute'] and date.second == d_dim['second']:
            return d_dim['date_key']

    return None



# Tarvitaan myös kaikista dim-tauluista keyt. Tällä vissiin haetaan vain user.key?
def _get_user_key(oltp_item, users):
    for user in users:
        if oltp_item['user_id'] == user['user_id']:
            return user['user_key']

    return None

# Tarvitaan myös kaikista dim-tauluista keyt. Tällä vissiin haetaan vain recipe.key?
def _get_recipe_key(oltp_item, recipes, key='id'):
    for recipe in recipes:
        if oltp_item[key] == recipe['recipe_id']:
            return recipe['recipe_key']

    return None


# Tyhjennys

def _clear_recipe_fact(_dw):
    _dw.execute(text('DELETE FROM recipe_fact'))
    _dw.commit()


def recipe_fact_etl():
    with get_db() as _db:
        recipes = _get_recipe_for_fact(_db)

    with get_db(cnx_type='olap') as _dw:
        try:

            # Haetaan kaikki tarvittavat tiedot olap-tietokannan dim-tauluista
            recipe_dims = _get_recipe_dims()
            user_dims = _get_user_dims()
            date_dims = _get_date_dims()

            # Loopataan kaikki recipet
            for recipe in recipes:
                # Haetaan foreign key arvot
                _date_key = _get_date_key(recipe, date_dims)
                _user_key = _get_user_key(recipe, user_dims)
                _recipe_key = _get_recipe_key(recipe, recipe_dims)
                if _date_key is None or _user_key is None or _recipe_key is None:
                    continue

                # Tehdään insertti
                _query_str = "INSERT INTO recipe_fact(created_at, fact_column, recipe, user) VALUES(:created_at, :fact_column, :recipe, :user)"
                _query = text(_query_str)
                _dw.execute(_query, {'created_at': _date_key, 'fact_column': 1, 'recipe': _recipe_key, 'user': _user_key})
            _dw.commit()

        except Exception as e:
            print(e)
            _dw.rollback()




# Haetaan oltp tietokannasta tietoa cooking facta taulua varten. Haetaan kaikki data.
def _get_cooking_for_fact(_db):
    rows = _db.execute(text('SELECT * FROM cooking'))
    return rows.mappings().all()


# Haetaand ate key cooking facta taulua varten
def _get_date_key_for_cooking_fact(oltp_item, dates):
    date = oltp_item['cooked_date']
    for d_dim in dates:
        if date.year == d_dim['year'] and date.month == d_dim['month'] and date.isocalendar().week == d_dim['week'
        ] and date.minute == d_dim['minute'] and date.second == d_dim['second']:
            return d_dim['date_key']

    return None



# Cooking tyhjennys
def _clear_cooking_fact(_dw):
    _dw.execute(text('DELETE FROM cooking_fact'))
    _dw.commit()


# cooking fact_taulu
def cooking_fact_etl():
    with get_db() as _db:
        cooking = _get_cooking_for_fact(_db)

    with get_db(cnx_type='olap') as _dw:
        try:
            recipe_dims = _get_recipe_dims()
            user_dims = _get_user_dims()
            date_dims = _get_date_dims()
            for c in cooking:
                _date_key = _get_date_key(c, date_dims, key='cooked_date')
                _user_key = _get_user_key(c, user_dims)
                _recipe_key = _get_recipe_key(c, recipe_dims, key='recipe_id')
                if _date_key is None or _user_key is None or _recipe_key is None:
                    continue


                _query_str = "INSERT INTO cooking_fact(date_cooked, user, recipe, rating) VALUES(:date_cooked, :user, :recipe, :rating)"
                _query = text(_query_str)
                _dw.execute(_query, {'date_cooked': _date_key, 'user': _user_key, 'recipe': _recipe_key, 'rating': c['rating']})

            _dw.commit()
        except Exception as e:
            print(e)
            _dw.rollback()