package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	"net/http"
)

var upgrader = websocket.Upgrader{
	//TODO: CheckOrigin is a function to allow connections from any origin. Should change this to allow connections from a specific origin.
	CheckOrigin: func(r *http.Request) bool { return true }, // allow all connections by default
}

var clients = make(map[*websocket.Conn]bool) // connected clients
var messageChannel = make(chan string)       // channel to send messages from websocket to main routine

func main() {
	http.HandleFunc("/ws", handleConnections)
	go broadcastMessages()

	fmt.Println("Starting server...")
	http.ListenAndServe(":8080", nil)
}
func handleConnections(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Websocket connection failed: ", err)
		return
	}
	defer conn.Close()
	clients[conn] = true // add connection to clients map
	fmt.Println("Websocket connected")
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("User disconnected from websocket: ", err)
			break
		}
		fmt.Println("Message received: ", string(msg))
		messageChannel <- string(msg) // send message to messageChannel
	}
}

func broadcastMessages() {
	for {
		msg := <-messageChannel
		for client := range clients {
			err := client.WriteMessage(websocket.TextMessage, []byte(msg))
			if err != nil {
				fmt.Println("Error broadcasting message: ", err)
				client.Close()
				delete(clients, client)
			}
		}
	}
}

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>
