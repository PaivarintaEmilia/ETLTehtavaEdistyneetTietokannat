from sqlalchemy import text

from db import get_db


def _get_recipe_dates(_db):
    # DISTINCT tarkoittaa, että jos on useaan kertaan sama data niin otetaan tämä data vain kerran
    _query_str = "SELECT DISTINCT created_at AS dt FROM recipe"
    _query = text(_query_str)
    rows = _db.execute(_query)

    date_rows = rows.mappings().all()
    dates = []

    for row in date_rows:
        dates.append(row['dt'])
    return dates


def _get_cooking_dates(_db):
    # DISTINCT tarkoittaa, että jos on useaan kertaan sama data niin otetaan tämä data vain kerran
    _query_str = "SELECT DISTINCT cooked_date AS dt FROM cooking"
    _query = text(_query_str)
    rows = _db.execute(_query)

    date_rows = rows.mappings().all()
    dates = []

    for row in date_rows:
        dates.append(row['dt'])
    return dates



def _clear_dates(_dw):
    _dw.execute(text('DELETE * FROM date_dim'))
    _dw.commit()


def date_etl():


    with get_db() as _db:
        # Haetaan haetut päivämäärät
        recipe_dates = _get_recipe_dates(_db)
        cooking_dates = _get_cooking_dates(_db)

        # Yhdistetään nämä kaikki tiedot
        all_dates = recipe_dates + cooking_dates
        print("############## all_dates", len(all_dates))

        # Ensin tehdään set ja setistä tehdään lista duplikaattien varalta (tämä tehdään all_dates muuttujalle)
        unique_dates = list(set(all_dates))
        print("Unique_dates", len(unique_dates))

    # OLAP
    with get_db(cnx_type='olap') as _dw:

        try:
            _clear_dates(_dw)

            _query_str = "INSERT INTO date_dim(year, month, week, day, hour, minute, second) " \
                         "VALUES(:year, :month, :week, :day, :hour, :minute, :second )"


            for date in unique_dates:
                _query = text(_query_str)
                _dw.execute(_query, {'year': date.year, 'month': date.month, 'week': date.isocalendar().week,
                                     'day': date.day, 'hour': date.hour, 'minute': date.minute, 'second': date.second})


            _dw.commit()
        except Exception as e:
            _dw.rollback()
            print(e)








