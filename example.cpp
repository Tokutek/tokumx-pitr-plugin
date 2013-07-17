// @file example.cpp

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

#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_details.h"
#include "mongo/plugins/plugins.h"
#include "mongo/util/log.h"

namespace mongo {

    namespace example {

        class ExampleCommand : public QueryCommand {
          public:
            ExampleCommand() : QueryCommand("pluginExample") {}
            virtual void help(stringstream &h) const {
                h << "reports the number of indexes for a collection" << endl
                  << "{ 'pluginExample': <collname> }";
            }
            virtual bool run(const string &db, BSONObj &cmdObj, int options, string &errmsg, BSONObjBuilder &result, bool fromRepl) {
                BSONElement e = cmdObj.firstElement();
                string ns = db + "." + e.str();
                NamespaceDetails *d = nsdetails(ns);
                if (d == NULL) {
                    errmsg = "ns not found";
                    return false;
                }
                result.append("ns", ns);
                result.append("indexes", d->nIndexes());
                return true;
            }
        };

        /*
         * One of the world's ugliest hacks.
         */
        class CommandRemover : private Command {
          public:
            static bool remove(const string &thename) {
                if (_commands == NULL) {
                    return false;
                }
                map<string, Command *>::iterator it = _commands->find(thename);
                if (it == _commands->end()) {
                    return false;
                }
                _commands->erase(it);
                return true;
            }
        };

        class ExampleInterface : public plugin::PluginInterface {
            vector<shared_ptr<Command> > _loadedCommands;
          public:
            virtual const string &name() const {
                static const string n = "exampleplugin";
                return n;
            }
            virtual void help(stringstream &h) const {
                h << "Here is some help text for the example plugin." << endl
                  << "It provides the following commands: " << endl;
                for (vector<shared_ptr<Command> >::const_iterator it = _loadedCommands.begin(); it != _loadedCommands.end(); ++it) {
                    h << (*it)->name << endl;
                }
            }
            virtual bool load(string &errmsg, BSONObjBuilder &result) {
                BSONArrayBuilder b(result.subarrayStart("newCommands"));
                shared_ptr<Command> cmd(new ExampleCommand);
                _loadedCommands.push_back(cmd);
                b.append(cmd->name);
                b.doneFast();
                return true;
            }
            virtual void unload(string &errmsg) {
                while (!_loadedCommands.empty()) {
                    shared_ptr<Command> cmd = _loadedCommands.back();
                    _loadedCommands.pop_back();
                    bool ok = CommandRemover::remove(cmd->name);
                    if (!ok) {
                        errmsg += "couldn't find command " + cmd->name;
                    }
                }
            }
        } exampleInterface;

    }

} // namespace mongo

extern "C" {

    __attribute__((visibility("default")))
    mongo::plugin::PluginInterface *getInterface(void) {
        return &mongo::example::exampleInterface;
    }

}
