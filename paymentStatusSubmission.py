# Importing Necessary modules
from fastapi import FastAPI
import plotly.graph_objects as go

# Declaring our FastAPI instance
app = FastAPI()

def get_mysqlconnection():
    import mysql.connector
    #connection credentials should be derived from the secret manager.
    #For demo purpose, I am giving hardcoded values.
    db = mysql.connector.connect(
        host="<servername>",
        user="<Username>",
        password="<Password>",
        database="<databasename>"
        , use_pure=True)
    return db
# This function connects to braintree API and fetches the trancation details
# for given time period. This is being stored in the mysql table for future use.
def getBrainTreePaymentStatus(start_date, end_date):
    try:
        import braintree
        import mysql.connector
        from datetime import datetime
        print("creating gateway for braintree:")
        #this part of the code should be derived from secret manager.
        #For the demo purpose, I have hardcoded the values
        gateway = braintree.BraintreeGateway(
            braintree.Configuration(
                environment=braintree.Environment.Sandbox,  # Replace with your environment (sandbox or production)
                merchant_id='<merchantid>',
                public_key='<publickey>',
                private_key='<privatekey>'
            )
        )
        print("creating mysql db connection:")
        # MySQL database connection
        db = get_mysqlconnection()
        cursor = db.cursor()
        print("got the mysql db connection. Fetching transaction details:")
        start_datetime = start_date
        end_datetime = end_date
        transactions = gateway.transaction.search(
            braintree.TransactionSearch.created_at.between(start_datetime, end_datetime)
        )
        result = transactions.items
        for transaction in result:
            if transaction.order_id is not None:
                # Storing transaction details into the MySQL database
                add_transaction = ("INSERT INTO ordertransactionlog "
                                   "(transactionid, orderid, amount, paymentstatus, tran_createddate, tran_updateddate, payment_method, isretry,paymentgateway) "
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s,'BrainTree')")
                transaction_data = (transaction.id, transaction.order_id, transaction.amount, transaction.status,
                                    transaction.created_at, transaction.updated_at, transaction.payment_instrument_type,
                                    transaction.retried)
                cursor.execute(add_transaction, transaction_data)
                db.commit()
    except ValueError as e:
        print("Invalid date format. Please use YYYY-MM-DD.")
    except braintree.exceptions.NotFoundError as e:
        print(f"Merchant ID not found.")
    except braintree.exceptions.AuthenticationError as e:
        print("Authentication failed. Check your credentials.")
    except braintree.exceptions.ServerError as e:
        print("A server error occurred. Please try again later.")

#given api key is not working, it will need similar code to identify the trancetion details
# and storing that data in the mysql database table.
def getStripePaymentStatus(start_date, end_date):
    try:
        print("Get the sample result from the stripe PG:")
        import stripe
        stripe.api_key = "<API KEY>"

        created = {'gte': '2023-12-26T00:00:00Z', 'lte': '2023-12-28T00:00:00'}

        pgresult = stripe.PaymentIntent.list(created=created, limit=10)
        print(pgresult)
    except stripe.error.StripeError as e:
        # Handle any errors that occur during the API request
        print(f"Error: {e}")


@app.get("/")
def getpaymentstats():

    retreivesql = "with cte_transactionstatus as \
                (select orderid, payment_method, dense_rank() over(partition by orderid order by tran_createddate ) as rnk,\
                amount,paymentstatus,paymentgateway\
                from ordertransactionlog ),\
                cte_retryinfo as\
                (select * from cte_transactionstatus where rnk <4 order by orderid)\
                select paymentgateway,\
                     payment_method,\
                     CASE WHEN rnk=1 THEN 'Attempt1'\
                          WHEN rnk=2 THEN 'Retry1'\
                          WHEN rnk=3 THEN 'Retry2' END as Attempts,\
                    COUNT(distinct (CASE WHEN paymentstatus='settled' then orderid else NULL END)) as success,\
                    COUNT(distinct (CASE WHEN paymentstatus<>'settled' then orderid else NULL END)) as Failed\
                from cte_retryinfo\
                group by paymentgateway,payment_method,rnk ; "
    db = get_mysqlconnection()
    cursor = db.cursor()
    cursor.execute(retreivesql)
    result = cursor.fetchall()
    for res in result:
        print(res)
    # df and pandas
    import pandas as pd
    sql_data = pd.DataFrame(result)
    sql_data.columns = cursor.column_names
    print(sql_data)
    cursor.close()

    nodes = pd.Index( sql_data['Attempts'].append(sql_data['paymentgateway']).append(sql_data['payment_method']).unique())
    node_index = {node: idx for idx, node in enumerate(nodes)}
    sql_data['source'] = sql_data['Attempts'].map(node_index)
    sql_data['target'] = sql_data['paymentgateway'].map(node_index)
    sql_data['target'] = sql_data['payment_method'].map(node_index)
    colors = ['pink' if s == 'success' else 'skyblue' for s in sql_data['Failed']]

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color=["pink", "skyblue"], width=0.5),
            label=nodes,
        ),
        link=dict(
            source=sql_data['source'],
            target=sql_data['target'],
            value=sql_data['success'] + sql_data['Failed'],  # Combined successes and failures
            label=sql_data['Attempts'],
            color=colors,
        ))])

    fig.update_layout(title_text="Payment Attempts and Statuses",
                      font=dict(size=12, color='Orange'),
                      height=500, width=800)

    fig.show()
    return sql_data.to_json()

'''    
if __name__ == '__main__':
    #Data retrival should be done only once for a given time period.
    #I havent added db check function for this. Below code should be used 
    # for getting data from stripe gateway for given time period
    getStripePaymentStatus('2023-12-26T00:00:00Z','2023-12-28T00:00:00Z')
    print("---------------------")
    #below code should be used for getting BrainTree data for given time period.
    getBrainTreePaymentStatus('2023-12-26T00:00:00Z','2023-12-28T00:00:00Z')
    #This is a test function to plot sankey plot and return the result
    # in json format. This sankey graph is not coming as expected. Due to time 
    #restriction, I have stopped updating this code.
    paymentStats=getpaymentstats()
    print(paymentStats)
'''
