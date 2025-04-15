using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Mutex
{
    class Mutex
    {
        private static bool _recursoEmUso = false;
        private static readonly object _lock = new object();

        static void Main(string[] args)
        {
            const int porta = 6000;
            TcpListener servidor = new TcpListener(IPAddress.Any, porta);
            servidor.Start();
            Console.WriteLine("[Mutex] Servidor iniciado na porta " + porta);

            try
            {
                while (true)
                {
                    TcpClient cliente = servidor.AcceptTcpClient();
                    ThreadPool.QueueUserWorkItem(TratarCliente, cliente);
                }
            }
            finally
            {
                servidor.Stop();
            }
        }

        static void TratarCliente(object state)
        {
            TcpClient cliente = (TcpClient)state;

            try
            {
                using (NetworkStream stream = cliente.GetStream())
                {
                    // Ler solicitação do cliente
                    byte[] buffer = new byte[1024];
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    string solicitacao = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();

                    if (solicitacao == "SOLICITAR")
                    {
                        lock (_lock)
                        {
                            if (!_recursoEmUso)
                            {
                                _recursoEmUso = true;
                                byte[] resposta = Encoding.UTF8.GetBytes("PERMITIDO\n");
                                stream.Write(resposta, 0, resposta.Length);
                                Console.WriteLine("[Mutex] Permissão concedida a um cliente");

                                // Esperar pela mensagem de liberação
                                bytesRead = stream.Read(buffer, 0, buffer.Length);
                                string libertar = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();

                                if (libertar == "LIBERTAR")
                                {
                                    _recursoEmUso = false;
                                    Console.WriteLine("[Mutex] Recurso libertado pelo cliente");
                                }
                            }
                            else
                            {
                                byte[] resposta = Encoding.UTF8.GetBytes("NEGADO\n");
                                stream.Write(resposta, 0, resposta.Length);
                                Console.WriteLine("[Mutex] Permissão negada - recurso em uso");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Mutex] Erro: {ex.Message}");
            }
            finally
            {
                cliente.Close();
            }
        }
    }
}