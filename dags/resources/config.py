QUERY_CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS public.movies(
            id integer,
            title varchar,
            genres varchar,
            original_language varchar,
            overview varchar,
            popularity varchar,
            production_companies varchar,
            release_date date,
            budget numeric,
            revenue numeric,
            runtime numeric,
            status varchar,
            tagline varchar,
            vote_average numeric,
            vote_count numeric,
            credits varchar,
            keywords varchar,
            poster_path varchar,
            backdrop_path varchar,
            recommendations varchar,
            inserted_at timestamp,
            primary key (id)
        )
    """

QUERY_REMOVE_DUPLICATES = """
    WITH cte AS (
                SELECT
                    id,
                    max(inserted_at) AS max_data
                FROM
                    public.movies
                GROUP BY
                    id
            )
            DELETE FROM public.movies t
            USING cte
            WHERE t.id = cte.id AND t.inserted_at <> cte.max_data
"""