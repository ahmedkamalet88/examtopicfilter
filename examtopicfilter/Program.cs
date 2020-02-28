using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace examtopicfilter
{
    class Program
    {
        const string connString = "Endpoint=sb://demoservice2020.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=sJq657lE4HTVu+508rMUgWANWIMv+JoSqJbDqd6vp+A=";
        const string topic_name = "demotopic";
        const string SubscriptionName = "receiveA";
        static SubscriptionClient l_subscriptionClient;
        static TopicClient l_topicClient;

        static void Main(string[] args)
        {
            MainFunction().GetAwaiter().GetResult();
        }
        static async Task MainFunction()
        {
            l_topicClient = new TopicClient(connString, topic_name);
            l_subscriptionClient = new SubscriptionClient(connString, topic_name, SubscriptionName);
            //SendMessage().Wait();
             ReceiveMessage().Wait();
            Console.ReadKey();
            
            //await l_subscriptionClient.CloseAsync();
        }
        static async Task SendMessage()
        {
            // Construct and encode the message
            string l_messageBody = "This is a sample message";
            var l_message = new Message(Encoding.UTF8.GetBytes(l_messageBody));
            
            l_message.MessageId = "1";
            l_message.UserProperties.Add("OrderCategory", "Mobile");
            Console.WriteLine("Sending the message");

            // Let the topic client send the message
            await l_topicClient.SendAsync(l_message);
            await l_topicClient.CloseAsync();
        }
        static async Task ReceiveMessage()
        {

            Console.WriteLine("Receiving the message");
            var l_Options = new MessageHandlerOptions(ExceptionHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };
            l_subscriptionClient.RegisterMessageHandler(MessageProcessor, l_Options);
        }
        static Task ExceptionHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            // This function will be called if there are any exceptions when receiving the messsage
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine(context.Action);
            return Task.CompletedTask;
        }
        static async Task MessageProcessor(Message message, CancellationToken token)
        {
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            Console.WriteLine(message.MessageId);
            object l_value;
            Console.WriteLine(message.UserProperties["OrderCategory"].ToString());

            // Complete the receival of the message so that it is not read by anyone else
            await l_subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
        }
    }
    }
