// @file pitr.cpp

/**
*    Copyright (C) 2013 Tokutek Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "mongo/pch.h"

#include <string>

#include "mongo/db/commands.h"
#include "mongo/plugins/command_loader.h"
#include "mongo/db/commands.h"
#include "mongo/db/repl/bgsync.h"


namespace mongo {

    namespace pitr {

        class CmdRecoverToPoint : public ReplSetCommand {
            void applyOperation(BSONObj curr, OplogReader& r) {
                GTID currEntry = getGTIDFromOplogEntry(curr);
                uint64_t ts = curr["ts"]._numberLong();
                uint64_t lastHash = curr["h"].numberLong();
                {
                    bool bigTxn = false;
                    Client::Transaction transaction(DB_SERIALIZABLE);
                    replicateFullTransactionToOplog(curr, r, &bigTxn);
                    // we are operating as a secondary. We don't have to fsync
                    transaction.commit(DB_TXN_NOSYNC);
                }
                theReplSet->gtidManager->noteGTIDAdded(currEntry, ts, lastHash);
                theReplSet->gtidManager->noteApplyingGTID(currEntry);
                applyTransactionFromOplog(curr, NULL);
                theReplSet->gtidManager->noteGTIDApplied(currEntry);
            }
        
            bool oplogEntryShouldBeApplied(BSONObj entry, GTID maxGTID, uint64_t maxTS) {
                uint64_t remoteTS = entry["ts"]._numberLong();
                GTID remoteGTID = getGTIDFromBSON("_id", entry);
                if (maxTS > 0 && remoteTS > maxTS) {
                    return false;
                }
                if (!maxGTID.isInitial() && GTID::cmp(remoteGTID, maxGTID) > 0) {
                    return false;
                }
                return true;
            }
        
        public:
            virtual bool canRunInMultiStmtTxn() const { return false; }
            virtual void help( stringstream &help ) const {
                help << "runs point-in-time recovery by syncing and applying oplog entries\n"
                     << "and stopping at the specified operation (identified by ts or gtid).\n"
                     << "Example: { " << name << " : 1, ts : <Date> } or { " << name << " : 1, gtid : <GTID> }";
            }
            virtual void addRequiredPrivileges(const std::string& dbname,
                                               const BSONObj& cmdObj,
                                               std::vector<Privilege>* out) {
                ActionSet actions;
                actions.addAction(ActionType::recoverToPoint);
                out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
            }
        
            CmdRecoverToPoint() : ReplSetCommand("recoverToPoint") { }
        
            // This command is not meant to be run in a concurrent manner. Assumes user is running this in
            // a controlled setting.
            virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
                BSONElement tse = cmdObj["ts"];
                BSONElement gtide = cmdObj["gtid"];
                GTID maxGTID;
                uint64_t maxTS = 0;
                if (tse.ok() == gtide.ok()) {
                    errmsg = "Must supply either gtid or ts, but not both";
                    return false;
                }

                if (tse.ok()) {
                    if (tse.type() != mongo::Date) {
                        errmsg = "Must supply a date for the ts field";
                        return false;
                    }
                    maxTS = tse._numberLong();
                }
                else if (gtide.ok()) {
                    // do some sanity checks
                    if (!isValidGTID(gtide)) {
                        errmsg = "gtid is not valid and cannot be parsed";
                        return false;
                    }
                    maxGTID = getGTIDFromBSON("gtid", cmdObj);
                }
                //
                // check we are in maintenance mode
                //
                if (!(theReplSet->state().recovering() && theReplSet->inMaintenanceMode())) {
                    errmsg = "Must be in recovering state (maintenance mode) to run recoverToPoint";
                    return false;
                }
        
                // now we are free to run point in time recovery.
                while (true) {
                    killCurrentOp.checkForInterrupt();
                    try {
                        const Member *source = NULL;
                        OplogReader r(false); // false, because we don't want to be contributing to write concern
                        string sourceHostname;
                        source = theReplSet->getMemberToSyncTo();
                        if (!source) {
                            log() << "could not get a member to sync from, sleeping 2 seconds" << endl;
                            sleepsecs(2);
                            continue;
                        }
                        sourceHostname = source->h().toString();
                        if( !r.connect(sourceHostname, 0) ) {
                            log() << "couldn't connect to " << sourceHostname << ", sleeping 2 seconds" << endl;
                            sleepsecs(2);
                            continue;
                        }
        
                        // make sure that the current location of the oplog is not past
                        // the point we wish to recover to
                        GTID lastGTIDFetched = theReplSet->gtidManager->getLiveState();
                        uint64_t currTS = theReplSet->gtidManager->getCurrTimestamp();
                        if (maxTS > 0 && currTS > maxTS) {
                            errmsg = str::stream() << "oplog is already past " << maxTS << ", it is at " << currTS;
                            return false;
                        }
                        if (!maxGTID.isInitial() && GTID::cmp(maxGTID, lastGTIDFetched) < 0) {
                            errmsg = str::stream() << "oplog is already past " << maxGTID.toString() << ", it is at " << lastGTIDFetched.toString();
                            return false;
                        }
        
                        r.tailingQueryGTE(rsoplog, lastGTIDFetched);
                        uint64_t ts;
                        if (isRollbackRequired(r, &ts)) {
                            errmsg = "Rollback is required, cannot continue to run operation";
                            return false;
                        }
        
                        // If we do not have more, we basically need to go try again
                        // One case where we may not have more is if the point in time
                        // we are recovering to is in the future. That's silly, but technically
                        // possible.
                        while (r.more()) {
                            killCurrentOp.checkForInterrupt();
        
                            BSONObj curr = r.nextSafe();
                            if (oplogEntryShouldBeApplied(curr, maxGTID, maxTS)) {
                                applyOperation(curr, r);
                            }
                            else {
                                return true;
                            }
                        }
                    }
                    catch (DBException& e) {
                        log() << "db exception when running point in time recovery: " << e.toString() << endl;
                        log() << "sleeping 1 second and continuing" << endl;
                        sleepsecs(1);
                    }
                    catch (std::exception& e2) {
                        log()  << "exception when running point in time recovery: " << e2.what() << endl;
                        log() << "sleeping 1 second and continuing" << endl;
                        sleepsecs(1);
                    }
                }
        
                return true;
            }
        };

        class PitrInterface : public plugins::CommandLoader {
          protected:
            CommandVector commands() const {
                CommandVector cmds;
                cmds.push_back(boost::make_shared<CmdRecoverToPoint>());
                return cmds;
            }
          public:
            const string &name() const {
                static const string n = "pitr_plugin";
                return n;
            }
            const string &version() const {
                static const string v = "0.0.1-pre-";
                return v;
            }
        } pitrInterface;
    }
} // namespace mongo

extern "C" {
    __attribute__((visibility("default")))
    mongo::plugins::PluginInterface *TokuMX_Plugin__getInterface(void) {
        return &mongo::pitr::pitrInterface;
    }
}
