
curl -X POST http://localhost:63803/api/messages/send -H "Content-Type: application/json" -d '{"messageCount": 10, "externalPayload": true, "useFlink": false, "payloadSize": 500000}'