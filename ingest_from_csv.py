import csv
import psycopg

password = "U(H3|Zy+\}osoO<M"

connection_params = {
    "host": "34.142.111.203",
    "dbname": "payments",
    "user": "postgres",
    "password": "new_password",
}

with psycopg.connect(
  "host=34.142.111.203 " +
    "dbname=payments " +
    "user=postgres " +
    f"password=new_password") as conn:
  with conn.cursor() as cur:
    cur.execute("""DROP TABLE IF EXISTS payments.users CASCADE;

    CREATE TABLE IF NOT EXISTS payments.users
    (
        user_id uuid NOT NULL,
        first_name character varying(64) COLLATE pg_catalog."default" NOT NULL,
        last_name character varying(64) COLLATE pg_catalog."default" NOT NULL,
        email character varying(64) COLLATE pg_catalog."default" NOT NULL,
        phone_number character varying(64) COLLATE pg_catalog."default" NOT NULL,
        street character varying(64) COLLATE pg_catalog."default" NOT NULL,
        postcode character varying(64) COLLATE pg_catalog."default" NOT NULL,
        town character varying(64) COLLATE pg_catalog."default" NOT NULL,
        county character varying(64) COLLATE pg_catalog."default" NOT NULL,
        country character varying(64) COLLATE pg_catalog."default" NOT NULL,
        user_iban character varying(64) COLLATE pg_catalog."default" NOT NULL,
        user_agent_bic character varying(64) COLLATE pg_catalog."default" NOT NULL,
        CONSTRAINT users_pkey PRIMARY KEY (user_id)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS payments.users
        OWNER to postgres;""")

    cur.execute("""DROP TABLE IF EXISTS payments.payments;

    CREATE TABLE IF NOT EXISTS payments.payments
    (
        payment_id uuid NOT NULL,
        message_id uuid NOT NULL,
        creation_date date NOT NULL,
        number_of_transactions integer NOT NULL,
        control_sum double precision NOT NULL,
        initiator_name character varying(64) COLLATE pg_catalog."default" NOT NULL,
        payment_information_id uuid NOT NULL,
        payment_method character varying(64) COLLATE pg_catalog."default" NOT NULL,
        batch_booking boolean NOT NULL,
        payment_count integer NOT NULL,
        payment_control_sum double precision NOT NULL,
        payment_service_level character varying(64) COLLATE pg_catalog."default" NOT NULL,
        local_instrument character varying(64) COLLATE pg_catalog."default" NOT NULL,
        sequence_type character varying(64) COLLATE pg_catalog."default" NOT NULL,
        requested_execution_date date NOT NULL,
        debtor_id uuid NOT NULL,
        instructed_amount double precision NOT NULL,
        currency character varying(64) COLLATE pg_catalog."default" NOT NULL,
        remittance_information character varying(64) COLLATE pg_catalog."default" NOT NULL,
        CONSTRAINT payments_pkey PRIMARY KEY (payment_id)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS payments.payments
        OWNER to postgres;""")

    conn.commit()

    with open('data/users.csv') as f:
      reader = csv.DictReader(f)
      i = 0
      for row in reader:
        i += 1
        print(f'inserted row {i}')
        cur.execute(
            "INSERT INTO payments.users (user_id, first_name, last_name, email, phone_number, street, postcode, town, county, country, user_iban, user_agent_bic) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            (row['user_id'], row['first_name'], row['last_name'], row['email'], row['phone_number'], row['street'], row['postcode'], row['town'], row['county'], row['country'], row['user_iban'], row['user_agent_bic']))

    conn.commit()

    with open('data/iso20022_payments.csv') as f:
      reader = csv.DictReader(f)
      i = 0
      for row in reader:
        i += 1
        print(f'inserted row {i}')
        cur.execute(
            "INSERT INTO payments.payments (payment_id, message_id, creation_date, number_of_transactions, control_sum, initiator_name, payment_information_id, payment_method, batch_booking, payment_count, payment_control_sum, payment_service_level, local_instrument, sequence_type, requested_execution_date, debtor_id, instructed_amount, currency, remittance_information) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            (row['payment_id'], row['message_id'], row['creation_date'], row['number_of_transactions'], row['control_sum'], row['initiator_name'], row['payment_information_id'], row['payment_method'], row['batch_booking'], row['payment_count'], row['payment_control_sum'], row['payment_service_level'], row['local_instrument'], row['sequence_type'], row['requested_execution_date'], row['debtor_id'], row['instructed_amount'], row['currency'], row['remittance_information']))

  conn.commit()
