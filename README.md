# AzureServiceBusSessionTester
POC to test the session capabilities of Azure.

# What Should Happen...
* ServiceBusSender will send 5 sets of 5 messages to the service bus. Each set of 5 messages has it's own session id.
* RendererReceiver will pick up those messages locked by the session id and process, sending a completion message back to the bus with the matching session id.
* ServiceBusSender will then process the status messages again locked by session.
* Run two instances of the receiver to see each instance only processing message by session.

# To Run
* Update the service bus connection string in both projects to your own Azure Service Bus.
* The apps will create all the necessary topics and subscriptions it needs.
* Build and run the RendererReceiver first.
* then with that still running run the ServiceBusSender.

# Note
* Note the use of Label, ReplyTo and ReplyToSessionId to configrue all the routing and filtering.
* Service Bus Explorer (SBE) **does not** show correlation filters, it can only show SQL Filters so the correlation filters used here will not show in SBE.
* It is currently configured to only process one session at a time. Search for "MaxConcurrentSessions" and change this to process more session in parallel.

# Useful Links
* https://docs.microsoft.com/en-us/azure/service-bus-messaging/topic-filters
* https://docs.microsoft.com/en-us/azure/service-bus-messaging/message-sessions
* https://weblogs.asp.net/sfeldman/asb-subs-with-correlation-filters