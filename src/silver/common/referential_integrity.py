from pyspark.sql.functions import *

def validate_product_with_category_supplier(product, suppliers, categories):

    parent_keys_category = categories.select(col('category_id')).distinct()
    parent_keys_supplier = suppliers.select(col('supplier_id')).distinct()

    valid_records = product.join(
        parent_keys_category, on='category_id', how='left_semi'
    ).join(
        parent_keys_supplier, on='supplier_id', how='left_semi'
    )

    invalid_category = product.join(
        parent_keys_category,
        on='category_id',
        how='left_anti'
    ).withColumn(
        'rejection_reason',
        lit('Invalid category_id reference')
    )

    invalid_supplier = product.join(
        parent_keys_supplier,
        on='supplier_id',
        how='left_anti'
    ).withColumn(
        'rejection_reason',
        lit('Invalid supplier_id reference')
    )
    invalid_records = (
        invalid_category
        .unionByName(
            invalid_supplier,
            allowMissingColumns=True
        )
    )
    return valid_records, invalid_records

def validate_inventory_with_product_warehouse(inventory, warehouses, products):

    parent_keys_warehouses = warehouses.select(col('warehouse_id')).distinct()
    parent_keys_products = products.select(col('product_id')).distinct()

    valid_records = inventory.join(
        parent_keys_warehouses, on='warehouse_id', how='left_semi'
    ).join(
        parent_keys_products, on='product_id', how='left_semi'
    )

    invalid_category = inventory.join(
        parent_keys_warehouses,
        on='warehouse_id',
        how='left_anti'
    ).withColumn(
        'rejection_reason',
        lit('Invalid warehouse_id reference')
    )

    invalid_products = inventory.join(
        parent_keys_products,
        on='product_id',
        how='left_anti'
    ).withColumn(
        'rejection_reason',
        lit('Invalid product_id reference')
    )
    invalid_records = (
        invalid_category
        .unionByName(
            invalid_products,
            allowMissingColumns=True
        )
    )
    return valid_records, invalid_records


def validate_orders_with_customers(orders, customers):

    parent_keys_customer = customers.select(col('customer_id')).distinct()

    valid_records = orders.join(
        parent_keys_customer, on='customer_id', how='left_semi'
    )

    invalid_records = orders.join(
        parent_keys_customer,
        on='customer_id',
        how='left_anti'
    ).withColumn(
        'rejection_reason',
        lit('Invalid customer_id reference')
    )
    return valid_records, invalid_records

def validate_orderitems_with_product_order_warehouse(order_items, warehouses, products, orders):

    parent_keys_warehouses = warehouses.select(col('warehouse_id')).distinct()
    parent_keys_products = products.select(col('product_id')).distinct()
    parent_keys_orders = orders.select(col('order_id')).distinct()

    valid_records = order_items.join(
        parent_keys_warehouses, on='warehouse_id', how='left_semi'
    ).join(
        parent_keys_products, on='product_id', how='left_semi'
    ).join(
        parent_keys_orders, on='order_id', how='left_semi'
    )

    invalid_warehouse = order_items.join(
        parent_keys_warehouses,
        on='warehouse_id',
        how='left_anti'
    ).withColumn(
        'rejection_reason',
        lit('Invalid warehouse_id reference')
    )

    invalid_products = order_items.join(
        parent_keys_products,
        on='product_id',
        how='left_anti'
    ).withColumn(
        'rejection_reason',
        lit('Invalid product_id reference')
    )

    invalid_orders = order_items.join(
        parent_keys_orders,
        on='order_id',
        how='left_anti'
    ).withColumn(
        'rejection_reason',
        lit('Invalid order_id reference')
    )

    invalid_records =(
        invalid_warehouse
        .unionByName(
            invalid_products,
            allowMissingColumns=True
        ).unionByName(
            invalid_orders,
            allowMissingColumns=True
        )
    )

    return valid_records, invalid_records

def validate_payments_with_order(payments, orders):
    parent_keys_orders = orders.select(col('order_id')).distinct()

    valid_records = payments.join(
        parent_keys_orders, on='order_id', how='left_semi'
    )

    invalid_records = payments.join(
        parent_keys_orders,
        on='order_id',
        how='left_anti'
    ).withColumn(
        'rejection_reason',
        lit('Invalid order_id reference')
    )
    return valid_records, invalid_records



