namespace Examen_Practico;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

public class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((hostContext, services) =>
            {
                // Registrar FileProcessingService como servicio hospedado
                services.AddHostedService<FileProcessingService>();
            })
            .UseWindowsService() // Configura para que se ejecute como un servicio de Windows
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole(); // Agrega salida de consola para depuración
            })
            .Build();

        await host.RunAsync();
    }
}
