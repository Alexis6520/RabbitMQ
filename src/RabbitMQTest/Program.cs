using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Collections.Concurrent;
using System.Text;

//ESTA DEMO FUNCIONA CON RabbitMQ.Client 6.8.1

//----------------- CREAR UNA CONEXIÓN -----------------
var hostName = "10.10.50.28";
var connFactory = new ConnectionFactory
{
    HostName = hostName,
    ClientProvidedName = "RabbitMQTestApp",
    AutomaticRecoveryEnabled = true,
};

//using var connection = connFactory.CreateConnection();
//connection.Close();

/*Tambien se puede pasar una lista de Endpoints en caso de que alguno no funcione, 
 * se conectará al primer endpoint disponible. Esto se puede realizar de la siguiente forma:*/
//var endpoints = new List<AmqpTcpEndpoint>
//{
//    new(hostName),
//    new("localhost")
//};

//connection = connFactory.CreateConnection(endpoints);
//connection.Close();
//connection.Dispose();
/* NOTA: No se recomienda crear una conexión por cada operación que se quiera realizar. 
 * Las conexiones están hechas para durar mucho tiempo. HAY QUE CERRAR ESTA CONEXIÓN ANTES DE DESECHARLA CON DISPOSE*/

//----------------- CREAR UN CANAL -----------------
//using var connection = connFactory.CreateConnection();
//using var channel = connection.CreateModel();
//channel.Close();
//connection.Close();
// NOTA: En caso de una excepción al usar el canal, este se cierra en automático y ya no se puede utilizar
// por lo que hay que crear un nuevo canal. HAY QUE CERRAR ESTA CONEXIÓN ANTES DE DESECHARLA CON DISPOSE 
// LOS CANALES NO SON MULTIHILOS POR LO QUE SE RECOMIENDA CREAR UN CANAL POR HILO

//----------------- MENSAJE DIRECTO A UNA COLA (Queue)-----------------
//using var connection = connFactory.CreateConnection();
//using var channel = connection.CreateModel();

//// Al declarar una cola se comprubeba que exista. En caso de no existir, se crea automáticamente
//var queueName = "hello";
//channel.QueueDeclare(queueName, true, false, false);
//var message = "Hola guapo";
//var body = Encoding.UTF8.GetBytes(message);
//var props = channel.CreateBasicProperties();
//props.Persistent = true; // Indica al Servidor que tiene que guardar el mensaje en disco
//channel.BasicPublish(string.Empty, queueName, body: body, basicProperties: props);
//channel.Close();
//connection.Close();

//----------------- MENSAJE A MULTIPLES COLAS A TRAVÉS DE UN INTERCAMBIADOR O PROPAGADOR-----------------
//using var connection = connFactory.CreateConnection();
//using var channel = connection.CreateModel();
//var exchangeName = "logs";
//channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout); // Crea el propagador de tipo Fanout(A todas las colas conocidas)
//var message = "Hola guapo";
//var body = Encoding.UTF8.GetBytes(message);
//var props = channel.CreateBasicProperties();
//props.Persistent = true; // Indica al Servidor que tiene que guardar el mensaje en disco
//channel.BasicPublish(exchangeName, string.Empty, props, body);
//channel.Close();
//connection.Close();
//Console.ReadLine();

//----------------- MENSAJE A MULTIPLES COLAS A TRAVÉS DE UN PROPAGADOR CON ETIQUETA DE TEMA-----------------
//using var connection = connFactory.CreateConnection();
//using var channel = connection.CreateModel();
//var exchangeName = "topic_logs";
//channel.ExchangeDeclare(exchangeName, ExchangeType.Topic); // Crea el propagador de tipo Fanout(A todas las colas conocidas)
//var message = "Hola guapo";
//var body = Encoding.UTF8.GetBytes(message);
//var props = channel.CreateBasicProperties();
//props.Persistent = true; // Indica al Servidor que tiene que guardar el mensaje en disco
//var topic = "a.orange.clockwork"; // *-Una palabra cualquiera,#-Una o más palabras
//channel.BasicPublish(exchangeName, topic, props, body);
//channel.Close();
//connection.Close();
//Console.ReadLine();

//----------------- CONFIRMACIÓN ASINCRONA DE PUBLICACIONES-----------------
//using var connection = connFactory.CreateConnection();
//using var channel = connection.CreateModel();
//channel.ConfirmSelect(); // Habilita la confirmación de publicaciones para este canal
//var exchangeName = "topic_logs";
//channel.ExchangeDeclare(exchangeName, ExchangeType.Topic); // Crea el propagador de tipo Fanout(A todas las colas conocidas)
//var message = "Hola guapo";
//var body = Encoding.UTF8.GetBytes(message);
//var props = channel.CreateBasicProperties();
//props.Persistent = true; // Indica al Servidor que tiene que guardar el mensaje en disco
//var topic = "a.orange.clockwork"; // *-Una palabra cualquiera,#-Una o más palabras
//var outstandingConfirms = new ConcurrentDictionary<ulong, int>(); // Alamacena los mensajes pendientes

//channel.BasicAcks += (sender, ea) => CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple); // Agrega un metodo al evento de confirmación

//// Agrega un método al evento de error
//channel.BasicNacks += (sender, ea) =>
//{
//    outstandingConfirms.TryGetValue(ea.DeliveryTag, out int body);
//    Console.WriteLine($"Message with body {body} has been nack-ed. Sequence number: {ea.DeliveryTag}, multiple: {ea.Multiple}");
//    CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple);
//};

//// Manda un lote de mensajes
//for (int i = 0; i < 10; i++)
//{
//    outstandingConfirms.TryAdd(channel.NextPublishSeqNo, i);
//    channel.BasicPublish(exchangeName, topic, props, body);
//}


//void CleanOutstandingConfirms(ulong sequenceNumber, bool multiple)
//{
//    if (multiple)
//    {
//        var confirmed = outstandingConfirms.Where(x => x.Key <= sequenceNumber);

//        foreach (var entry in confirmed)
//        {
//            outstandingConfirms.TryRemove(entry.Key, out _);
//        }
//    }
//    else
//    {
//        outstandingConfirms.TryRemove(sequenceNumber, out _);
//    }
//}

//channel.Close();
//connection.Close();
//Console.ReadLine();

//----------------- CONFIRMACIÓN ASINCRONA DE UN LOTE PUBLICACIONES-----------------
using var connection = connFactory.CreateConnection();
using var channel = connection.CreateModel();
channel.ConfirmSelect(); // Habilita la confirmación de publicaciones para este canal
var exchangeName = "topic_logs";
channel.ExchangeDeclare(exchangeName, ExchangeType.Topic); // Crea el propagador de tipo Fanout(A todas las colas conocidas)
var message = "Hola guapo";
var body = Encoding.UTF8.GetBytes(message);
var props = channel.CreateBasicProperties();
props.Persistent = true; // Indica al Servidor que tiene que guardar el mensaje en disco
var topic = "a.orange.clockwork"; // *-Una palabra cualquiera,#-Una o más palabras

// Manda un lote de mensajes
for (int i = 0; i < 10; i++)
{
    channel.BasicPublish(exchangeName, topic, props, body);
}

try
{
    channel.WaitForConfirmsOrDie(); // Espera a que se hayan confirmado todos los mensajes
}
catch (OperationInterruptedException)
{
    // Maneja en caso de recibir nack
}

Console.ReadLine();
channel.Close();
connection.Close();