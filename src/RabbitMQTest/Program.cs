using RabbitMQ.Client;
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

//----------------- CONFIRMACIÓN DE PUBLICACIONES-----------------
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

// Espera a que el servidor confirme de recibido
try
{
    channel.WaitForConfirmsOrDie();
}
catch (Exception)
{
    // Maneja el error
}

channel.Close();
connection.Close();
Console.ReadLine();