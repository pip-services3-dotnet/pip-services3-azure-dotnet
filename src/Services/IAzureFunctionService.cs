#if NETCOREAPP3_1_OR_GREATER

using System.Collections.Generic;

namespace PipServices3.Azure.Services
{
    /// <summary>
    /// An interface that allows to integrate Azure Function services into Azure Function containers
    /// and connect their actions to the function calls.
    /// </summary>
    public interface IAzureFunctionService
    {
        /// <summary>
        /// Get all actions supported by the service.
        /// </summary>
        /// <returns>an array with supported actions.</returns>
        IList<AzureFunctionAction> GetActions();
    }
}

#endif