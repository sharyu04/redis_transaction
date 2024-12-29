package main

import (
	"context"
	"fmt"
	"log"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// Seed data: Product IDs and their quantities
	products := map[string]int{
		// Product ID: Quantity
		"product:1001": 50,
		"product:1002": 30,
	}

	// Store product quantities in Redis
	for productID, quantity := range products {
		err := client.Set(ctx, productID, quantity, 0).Err() // Set product ID as key, quantity as value
		if err != nil {
			log.Fatalf("Error storing product %s: %v", productID, err)
		}
	}

	// Define order details
	productID := "product:1001"
	orderID := "order:001"
	requiredQuantity := 5

	// Lua script for atomic stock check and order creation
	luaScript := `
		-- KEYS[1] is the product ID
		-- KEYS[2] is the order ID
		-- ARGV[1] is the required quantity

		local product_stock = redis.call("GET", KEYS[1])
		if not product_stock then
			return redis.error_reply("Product not found")
		end

		if tonumber(product_stock) < tonumber(ARGV[1]) then
			return redis.error_reply("Insufficient stock")
		end

		-- Reduce stock
		redis.call("DECRBY", KEYS[1], ARGV[1])

		-- Create order (store order ID and product ID)
		redis.call("SET", KEYS[2], KEYS[1])

		return "Order placed successfully"
	`

	// Execute the Lua script in Redis
	result, err := client.Eval(ctx, luaScript, []string{productID, orderID}, requiredQuantity).Result()
	if err != nil {
		log.Fatalf("Error executing Lua script: %v", err)
	} else {
		fmt.Println(result)
	}
}
