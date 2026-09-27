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

#include "ModuleDatabasePool.h"
#include "AdhocStatement.h"
#include "Errors.h"
#include "Log.h"
#include "MySQLConnection.h"
#include "MySQLPreparedStatement.h"
#include "PCQueue.h"
#include "PreparedStatement.h"
#include "QueryResult.h"
#include "SQLOperation.h"
#include "Transaction.h"
#include <errmsg.h>
#include <limits>
#include <mysqld_error.h>
#include <thread>

namespace
{
class ModulePingOperation : public SQLOperation
{
    bool Execute() override
    {
        m_conn->Ping();
        return true;
    }
};
}

ModuleDatabasePool::ModuleDatabasePool()
    : _connectionInfo(""), _queue(std::make_unique<ProducerConsumerQueue<SQLOperation*>>()), _asyncThreads(0),
      _synchThreads(0)
{
}

ModuleDatabasePool::~ModuleDatabasePool()
{
    Close();
}

void ModuleDatabasePool::SetConnectionInfo(std::string_view infoString, uint8 asyncThreads, uint8 synchThreads)
{
    _connectionInfo = MySQLConnectionInfo(infoString);
    _asyncThreads = asyncThreads;
    _synchThreads = synchThreads;
}

uint32 ModuleDatabasePool::Open()
{
    if (!_asyncThreads || !_synchThreads)
    {
        LOG_ERROR("sql.driver", "ModuleDatabasePool: database `{}` requires at least one asynchronous and one "
            "synchronous connection (configured: {} asynchronous, {} synchronous).", _connectionInfo.database,
            _asyncThreads, _synchThreads);
        return CR_UNKNOWN_ERROR;
    }

    Close();
    _queue->Reset();

    uint32 result = OpenConnections(IDX_ASYNC, _asyncThreads);
    if (result)
        return result;

    result = OpenConnections(IDX_SYNCH, _synchThreads);
    if (result)
        return result;

    return 0;
}

bool ModuleDatabasePool::PrepareStatements()
{
    for (auto const& connections : _connections)
    {
        for (auto const& conn : connections)
        {
            conn->LockIfReady();
            if (!conn->PrepareStatements())
            {
                conn->Unlock();
                Close();
                return false;
            }

            conn->Unlock();

            std::size_t const preparedSize = conn->m_stmts.size();
            if (_preparedStatementSize.size() < preparedSize)
                _preparedStatementSize.resize(preparedSize);

            for (std::size_t i = 0; i < preparedSize; ++i)
            {
                if (_preparedStatementSize[i] > 0)
                    continue;

                if (MySQLPreparedStatement* stmt = conn->m_stmts[i].get())
                {
                    uint32 const paramCount = stmt->GetParameterCount();
                    ASSERT(paramCount < std::numeric_limits<uint8>::max());
                    _preparedStatementSize[i] = static_cast<uint8>(paramCount);
                }
            }
        }
    }

    return true;
}

void ModuleDatabasePool::Close()
{
    _queue->Shutdown();
    _connections[IDX_ASYNC].clear();
    _connections[IDX_SYNCH].clear();

    _preparedStatementSize.clear();
}

void ModuleDatabasePool::Execute(std::string_view sql)
{
    if (sql.empty() || _connections[IDX_ASYNC].empty())
        return;

    Enqueue(new BasicStatementTask(sql));
}

void ModuleDatabasePool::DirectExecute(std::string_view sql)
{
    if (sql.empty())
        return;

    if (_connections[IDX_SYNCH].empty())
        return;

    MySQLConnection* conn = GetFreeConnection();
    conn->Execute(sql);
    conn->Unlock();
}

QueryResult ModuleDatabasePool::Query(std::string_view sql)
{
    if (_connections[IDX_SYNCH].empty())
        return QueryResult(nullptr);

    MySQLConnection* conn = GetFreeConnection();
    ResultSet* result = conn->Query(sql);
    conn->Unlock();

    // Mirror DatabaseWorkerPool<T>::Query semantics: nullptr for empty results,
    // and the first row loaded before the result is handed out.
    if (!result || !result->GetRowCount() || !result->NextRow())
    {
        delete result;
        return QueryResult(nullptr);
    }

    return QueryResult(result);
}

void ModuleDatabasePool::Execute(PreparedStatementBase* stmt)
{
    if (_connections[IDX_ASYNC].empty())
    {
        delete stmt;
        return;
    }

    Enqueue(new PreparedStatementTask(stmt));
}

PreparedQueryResult ModuleDatabasePool::Query(PreparedStatementBase* stmt)
{
    if (_connections[IDX_SYNCH].empty())
    {
        delete stmt;
        return PreparedQueryResult(nullptr);
    }

    MySQLConnection* conn = GetFreeConnection();
    PreparedResultSet* result = conn->Query(stmt);
    conn->Unlock();

    //! Delete proxy-class. Not needed anymore
    delete stmt;

    if (!result || !result->GetRowCount())
    {
        delete result;
        return PreparedQueryResult(nullptr);
    }

    return PreparedQueryResult(result);
}

uint8 ModuleDatabasePool::GetPreparedStatementParamCount(uint32 index) const
{
    return index < _preparedStatementSize.size() ? _preparedStatementSize[index] : 0;
}

void ModuleDatabasePool::CommitTransaction(std::shared_ptr<TransactionBase> transaction)
{
    if (_connections[IDX_ASYNC].empty())
        return;

    Enqueue(new TransactionTask(std::move(transaction)));
}

void ModuleDatabasePool::DirectCommitTransaction(std::shared_ptr<TransactionBase> transaction)
{
    if (_connections[IDX_SYNCH].empty())
        return;

    MySQLConnection* conn = GetFreeConnection();
    int errorCode = conn->ExecuteTransaction(transaction);
    if (!errorCode)
    {
        conn->Unlock();
        return;
    }

    //! Handle MySQL Errno 1213 without extending deadlock to the core itself
    if (errorCode == ER_LOCK_DEADLOCK)
    {
        uint8 constexpr loopBreaker = 5;
        for (uint8 i = 0; i < loopBreaker; ++i)
        {
            if (!conn->ExecuteTransaction(transaction))
                break;
        }
    }

    transaction->Cleanup();
    conn->Unlock();
}

void ModuleDatabasePool::KeepAlive()
{
    //! Ping connections that are not busy; a locked connection is in use and alive.
    for (auto const& conn : _connections[IDX_SYNCH])
    {
        if (conn->LockIfReady())
        {
            conn->Ping();
            conn->Unlock();
        }
    }

    for (std::size_t i = 0; i < _connections[IDX_ASYNC].size(); ++i)
        Enqueue(new ModulePingOperation());
}

std::size_t ModuleDatabasePool::QueueSize() const
{
    return _queue->Size();
}

uint32 ModuleDatabasePool::OpenConnections(InternalIndex type, uint8 numConnections)
{
    for (uint8 i = 0; i < numConnections; ++i)
    {
        auto conn = type == IDX_ASYNC
            ? std::unique_ptr<MySQLConnection>(CreateConnection(_queue.get(), _connectionInfo))
            : std::unique_ptr<MySQLConnection>(CreateConnection(_connectionInfo));
        uint32 result = conn->Open();
        if (result != 0)
        {
            LOG_ERROR("sql.driver", "ModuleDatabasePool: could not open {} connection {}/{} to database `{}`, error {}",
                type == IDX_ASYNC ? "asynchronous" : "synchronous", i + 1, numConnections,
                _connectionInfo.database, result);
            Close();
            return result;
        }

        _connections[type].push_back(std::move(conn));
    }

    return 0;
}

void ModuleDatabasePool::Enqueue(SQLOperation* operation)
{
    _queue->Push(operation);
}

MySQLConnection* ModuleDatabasePool::GetFreeConnection()
{
    uint8 i = 0;
    auto const numCons = _connections[IDX_SYNCH].size();
    MySQLConnection* connection = nullptr;

    //! Block forever until a connection is free
    for (;;)
    {
        connection = _connections[IDX_SYNCH][++i % numCons].get();
        //! Must be matched with connection->Unlock() or you will get deadlocks
        if (connection->LockIfReady())
            break;

        if (i % numCons == 0)
            std::this_thread::yield();
    }

    return connection;
}

MySQLConnectionInfo const* ModuleDatabasePool::GetConnectionInfo() const
{
    return &_connectionInfo;
}
