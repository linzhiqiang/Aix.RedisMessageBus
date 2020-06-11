using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.RedisMessageBus
{
    public interface IBackgroundProcess:IDisposable
    {
        Task Execute(BackgroundProcessContext context);
    }


}
