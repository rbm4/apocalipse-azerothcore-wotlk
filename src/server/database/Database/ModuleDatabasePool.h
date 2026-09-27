/*
 * This file is part of the AzerothCore Project. See AUTHORS file for Copyright information
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef MODULE_DATABASE_POOL_H
#define MODULE_DATABASE_POOL_H

#include "DatabaseEnvFwd.h"
#include "DatabaseUpdatePool.h"
#include "Define.h"
#include "MySQLConnection.h"
#include "PreparedStatement.h"
#include "StringFormat.h"
#include <array>
#include <memory>
#include <string_view>
#include <vector>

template <typename T>
class ProducerConsumerQueue;

class SQLOperation;
class TransactionBase;

// Base class for module-owned database pools. A module derives from this,
// implements CreateConnection with its own MySQLConnection subclass (carrying
// the module's prepared statements), and gets open/execute/query plus DBUpdater
// compatibility without any core-side registration.
//
// Execute and CommitTransaction use asynchronous connections, matching
// DatabaseWorkerPool semantics. Query, DirectExecute and DirectCommitTransaction
// use synchronous connections.
class AC_DATABASE_API ModuleDatabasePool : public DatabaseUpdatePool
{
public:
    ModuleDatabasePool();
    virtual ~ModuleDatabasePool();

    void SetConnectionInfo(std::string_view infoString, uint8 asyncThreads, uint8 synchThreads);

    //! Opens the configured asynchronous and synchronous connections.
    //! Returns 0 on success, or the MySQL error code of the first failed connection.
    uint32 Open();

    //! Prepares the connection statements. Call after the schema exists
    //! (post create/populate/update), mirroring DatabaseLoader's ordering.
    bool PrepareStatements();

    void Close();

    void Execute(std::string_view sql);
    void DirectExecute(std::string_view sql) override;
    QueryResult Query(std::string_view sql) override;
    MySQLConnectionInfo const* GetConnectionInfo() const override;

    //! Format variants, mirroring DatabaseWorkerPool.
    template<typename... Args>
    void Execute(std::string_view sql, Args&&... args)
    {
        if (sql.empty())
            return;

        Execute(std::string_view(Acore::StringFormat(sql, std::forward<Args>(args)...)));
    }

    template<typename... Args>
    void DirectExecute(std::string_view sql, Args&&... args)
    {
        if (sql.empty())
            return;

        DirectExecute(std::string_view(Acore::StringFormat(sql, std::forward<Args>(args)...)));
    }

    template<typename... Args>
    QueryResult Query(std::string_view sql, Args&&... args)
    {
        if (sql.empty())
            return QueryResult(nullptr);

        return Query(std::string_view(Acore::StringFormat(sql, std::forward<Args>(args)...)));
    }

    //! Prepared statements. The index space is defined by the module's connection
    //! class (DoPrepareStatements); parameter counts are recorded by PrepareStatements(),
    //! so building one before that call yields a zero-parameter statement.
    //! Both calls consume (delete) the statement, mirroring DatabaseWorkerPool.
    void Execute(PreparedStatementBase* stmt);
    PreparedQueryResult Query(PreparedStatementBase* stmt);

    //! Parameter count for a prepared statement index, for constructing typed
    //! PreparedStatement<T> objects module-side.
    [[nodiscard]] uint8 GetPreparedStatementParamCount(uint32 index) const;

    //! Enqueues the transaction for execution on an asynchronous connection.
    void CommitTransaction(std::shared_ptr<TransactionBase> transaction);

    //! Synchronously commits the transaction on a free connection.
    void DirectCommitTransaction(std::shared_ptr<TransactionBase> transaction);

    //! Pings every idle connection to keep them alive.
    void KeepAlive();

    [[nodiscard]] std::size_t QueueSize() const;

protected:
    virtual MySQLConnection* CreateConnection(MySQLConnectionInfo& connInfo) = 0;
    virtual MySQLConnection* CreateConnection(ProducerConsumerQueue<SQLOperation*>* queue,
                                              MySQLConnectionInfo& connInfo) = 0;

private:
    enum InternalIndex
    {
        IDX_ASYNC,
        IDX_SYNCH,
        IDX_SIZE
    };

    uint32 OpenConnections(InternalIndex type, uint8 numConnections);
    void Enqueue(SQLOperation* operation);
    MySQLConnection* GetFreeConnection();

    MySQLConnectionInfo _connectionInfo;
    std::unique_ptr<ProducerConsumerQueue<SQLOperation*>> _queue;
    std::array<std::vector<std::unique_ptr<MySQLConnection>>, IDX_SIZE> _connections;
    std::vector<uint8> _preparedStatementSize;
    uint8 _asyncThreads;
    uint8 _synchThreads;
};

#endif
