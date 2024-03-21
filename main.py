# user dimin data miten saadaan?
# Eli tarkotuksena on kai hakea dataa oltp-tietokannasta, jotta sitä voidaan lisätä olap tietokantaan
from sqlalchemy import text

from db import get_db


# Haetaan käyttäjät lähdetietokannasta user_etl funktiota varten. MUOKKAA TÄTÄ OMAAN TYÖHÖN SOPIVAKSI
def _get_users(_db):
    _query = text('SELECT users.id AS user_id, username, auth_roles.id as role_id, role'
                  'FROM users'
                  'INNER JOIN auth_roles ON auth_roles.id = users.auth_role_id')
    rows = _db.execute(_query)

    users = rows.mappings().all()

    return users

def _get_recipes(_db):
    # Tähän tulee query, jolla haetaan kaikki tarvittava tieto tiettyä dim-taulua varten. Voi olla, että tietoa tarvitsee hakea useammasta taulusta.
    _query_str = "SELECT recipe.id AS recipe_id, recipe.name, users.username AS user, users.id AS user_id, categories.id AS category_id, categories.name AS category " \
                 "FROM recipe" \
                 "INNER JOIN users ON recipe.user_id = users.id" \
                 "INNER JOIN categories ON recipe.category.id = categories.id"

    _query = text(_query_str)

    rows = _db.execute(_query)

    return rows.mappings().all()


def _clear_users(_dw):
    try:
        _query = text("DELETE * FROM user_dim")
        _dw.execute(_query)
        _dw.commit()
    except Exception as e:
        print(e)


def _clear_recipes(_dw):
    try:
        _query = text("DELETE * FROM recipes_dim")
        _dw.execute(_query)
        _dw.commit()
    except Exception as e:
        print(e)

# Funktio, jolla siirretään dataa vai funktio, jolla haetaan dataa? Ja mistä tietokannasta?
def user_etl():
    # Ensin tehdään tietokantayhteys oltp-tietokantaan, niin saadaan siirrettävät tiedot
    with get_db() as _db:
        users = _get_users(_db)

    # Aikaisempaa yhteyttä ei enää tarvita niin avataan uusi yhteys olap tietokantaan
    with get_db(cnx_type='olap') as _dw:
        try:
            # Tyhjennetään jos ajetaan ohjelma moneen kertaan niin ei mene päällekkäin data
            _clear_users(_dw)
            # Loopataan oltp tietokannasta saadut tulokset tässä
            for user in users:
                _query = text('INSERT INTO user_dim(user_id, username, role_id, role, current) '
                              'VALUES(:user_id, :username, :role_id, :role, :current)')

                _dw.execute(_query, {'user_id': user['user_id'], 'username': user['username'], 'role_id': user['role_id'],
                                     'role': user['role'], 'current': 1})

            _dw.commit()

        except Exception as e:
            # Jos kysely epäonnistuu niin rollback peruuttaa muutokset
            _dw.rollback()
            print(e)



# 2. video - Recipe taulun täyttö
def recipe_etl():

    # Tehdään yhteys oltp-tietokantaan, jotta sadadaan siirrettävä data. Datan keräys tehdään toisessa funktiossa.
    with get_db() as _db:
        recipes = _get_recipes(_db)

    with get_db(cnx_type='olap') as _dw:
        try:
            # Tyhjennetään jos ajetaan ohjelma moneen kertaan niin ei mene päällekkäin data
            _clear_recipes(_dw)
            for recipe in recipes:
                _query_str = "INSERT INTO recipe_dim(recipe_id, name, user, user_id, category_id, category, current) " \
                             "VALUES(:recipe_id, :name, :user, :user_id, :category_id, :category, :current)"

                _query = text(_query_str)
                _dw.execute(_query, {'recipe_id': recipe['recipe_id'], 'name': recipe['name'], 'user': recipe['user'],
                                    'user_id': recipe['user_id'], 'category_id': recipe['category_id'],
                                     'category': recipe['category'], 'current': 1})

                _dw.commit()
        except Exception as e:
            _dw.rollback()
            print(e)



def main():
    user_etl() #
    recipe_etl()


if __name__ == '__main__':
    main()