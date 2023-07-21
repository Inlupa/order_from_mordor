import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings("ignore")

# getting path for initial json file
path = os.path.basename(__file__)
path = os.path.abspath(__file__).replace(path, '')

# reading json into DF
orders = pd.read_json(path + '//trial_task.json', encoding="utf-8")


def unpack(el: list) -> "pd.DataFrame":
    """ Unpacking "products" and joining them for order_id

    Args:
        el (list): List of json elements

    Returns:
        pd.DataFrame: DataFrame with products list as columns
    """
    order_id, warehouse_name, highway_cost, produts = el
    left_df = pd.DataFrame(
        [(order_id, warehouse_name, highway_cost)],
        columns=['order_id', 'warehouse_name', 'highway_cost'])

    return left_df.join(pd.DataFrame.from_dict(produts), how='cross')


res_df = pd.DataFrame()

# joining all order_ids together in one DF
for el in orders.values:
    res_df = pd.concat([res_df, unpack(el)])

res_df.reset_index(inplace=True, drop=True)


def get_warehouse_cost(df: 'pd.DataFrame') -> 'pd.DataFrame':
    """getting orders for each warehouse

    Args:
        df (pd.DataFrame): DF with all products 

    Returns:
        pd.DataFrame: DF with unique warehouses
    """
    warehouse_order = df[df['order_id'] == df['order_id'].unique()[0]]
    return warehouse_order['highway_cost'].unique(
    )[0]/warehouse_order['quantity'].sum()


# resulting DF with each warehouses' cost
warehouse_cost = pd.DataFrame(res_df.groupby(['warehouse_name']).apply(
    get_warehouse_cost), columns=['warehouse_cost']).reset_index()

# saving task 1 to a file 
warehouse_cost.to_csv(path + "//task1.txt", sep='\t', index=False)

# second task

res_df_cost = res_df.set_index('warehouse_name').join(
    warehouse_cost.set_index('warehouse_name'))

res_df_cost['expense'] = res_df_cost[
    'warehouse_cost'] * res_df_cost['quantity']

res_df_cost.reset_index(inplace=True)

product_sum = res_df_cost.groupby(['product']).sum().reset_index()
product_sum['profit'] = product_sum['price'] - product_sum['expense']
product_sum = product_sum[[
    'product', 'quantity', 'price', 'expense', 'profit'
    ]]

# saving task 2 to a file
product_sum.to_csv(path + "//task2.txt", sep='\t', index=False)

# third task

res_df_cost['order_price'] = res_df_cost['price'] * res_df_cost['quantity']

order_profit_df = res_df_cost.groupby([
    'order_id', 'highway_cost']).sum().reset_index()

order_profit_df['order_profit'] = order_profit_df[
    'order_price'] + order_profit_df['highway_cost']

order_profit_df = order_profit_df[['order_id', 'order_profit']]

# saving task 3 to a file 
order_profit_df.to_csv(path + "//task3.txt", sep='\t', index=False)
with open(path + "//task3.txt", 'a') as f:
    f.write('\n')
    f.write("Средняя прибыль заказов " + str(
        order_profit_df['order_profit'].mean()))

# tasks 4 - 6

# group by  warehouse_name and product
warehouse_profit = res_df_cost.groupby(
    ['warehouse_name', 'product']).sum().reset_index()
# calculating product profit inside warehouse
warehouse_profit['profit'] = warehouse_profit[
    'order_price'] + warehouse_profit['expense']

# DF with warehouse_profit, calculating warehouse profit inside warehouse
warehouse_profit = warehouse_profit.set_index('warehouse_name').join(
    warehouse_profit.groupby(['warehouse_name']).sum().rename(
        columns={'profit': 'profit_warehouse'})[
            'profit_warehouse']).reset_index()

# calculating percent_profit_product_of_warehouse
warehouse_profit['percent_profit_product_of_warehouse'] = (
    warehouse_profit['profit']/warehouse_profit['profit_warehouse']) * 100

# resulting DF
warehouse_profit = warehouse_profit[[
    'warehouse_name',
    'product',
    'quantity',
    'profit',
    'percent_profit_product_of_warehouse']]

warehouse_profit = warehouse_profit.sort_values(
    by=['warehouse_name',
        'percent_profit_product_of_warehouse'], ascending=False).reset_index(
            drop=True)
x = 0
accumulated = []
warehouse_profit[
    'accumulated_percent_profit_product_of_warehouse'] = warehouse_profit[
        'percent_profit_product_of_warehouse']
# creating list of accumulating percents
for el in warehouse_profit['accumulated_percent_profit_product_of_warehouse'].values:
    x += el
    accumulated.append(x)
    # going arround python round errors
    if (x % 100 < 0.001) | (x % 100 > 99.999):
        x = 0
warehouse_profit[
    'accumulated_percent_profit_product_of_warehouse'] = accumulated

warehouse_profit['category'] = 'B'
warehouse_profit.loc[warehouse_profit[
    warehouse_profit.accumulated_percent_profit_product_of_warehouse <= 70].index, 'category'] = 'A'

warehouse_profit.loc[warehouse_profit[
    warehouse_profit.accumulated_percent_profit_product_of_warehouse > 90].index, 'category'] = 'C'

warehouse_profit.to_csv(path + "//task456.txt", sep='\t', index=False)
