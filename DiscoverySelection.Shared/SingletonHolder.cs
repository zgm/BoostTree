using System;
using System.Collections.Generic;
using System.Text;

namespace DiscoverySelection.Shared
{
    /// <summary>
    /// Simple wrapper class that allows creation of singletons
    /// </summary>
    public sealed class SingletonHolder<T> where T : class, new()
    {
        private static T instance = null;
        public static T Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new T();
                }

                return instance;
            }
        }

        private SingletonHolder()
        {
        }
    }
}
