import psycopg2
from connect_pg import host, user, db_name, password, port


def main():
    # connect
    connection = psycopg2.connect(
        host=host, user=user, password=password, port=port, database=db_name
    )

    # cursor
    cursor = connection.cursor()

    cursor.execute(
        """create table if not exists aggr_ord_sale (
                    date_order TIMESTAMP WITHOUT TIME ZONE,
                    ord_sum INT,
                    sale_sum INT,
                    ord_cnt INT,
                    sale_cnt INT,
					ctl_datetime TIMESTAMP WITHOUT TIME ZONE
                )"""
    )

    cursor.execute("""truncate table aggr_ord_sale""")

    cursor.execute(
        """ insert into aggr_ord_sale
            select
                date(date_order) as date_order,
                sum(case when type='orders' then pricewithdisc else 0 end) as ord_sum,
                sum(case when type='sales' then pricewithdisc else 0 end) as sale_sum,
                sum(case when type='orders' then 1 else 0 end) as ord_cnt,
                sum(case when type='sales' then 1 else 0 end) as sale_cnt,
                now() as ctl_datetime
            from
                (select
                    date_order,
                    pricewithdisc,
                    'sales' as type
                from sales
                union
                select
                    date_order,
                    pricewithdisc,
                    'orders' as type
                from orders) t
            group by date(date_order)
            order by date(date_order)"""
    )

    connection.commit()

    cursor.close()
    connection.close()
    print("closed")


if __name__ == "__main__":
    main()
