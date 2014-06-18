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
#include "mongo/plugins/command_loader.h"

namespace mongo {

    namespace example {

        class ExampleCommand : public QueryCommand {
          public:
            ExampleCommand() : QueryCommand("pluginExample") {}
            virtual void help(stringstream &h) const {
                h << "says hello world" << endl
                  << "{ 'pluginExample': 1 }";
            }
	  virtual void addRequiredPrivileges(const std::string& dbname,
					     const BSONObj& cmdObj,
					     std::vector<Privilege>* out) {}
            virtual bool run(const string &db, BSONObj &cmdObj, int options, string &errmsg, BSONObjBuilder &result, bool fromRepl) {
                result.append("hello", "world");
                return true;
            }
        };

        class ExampleInterface : public plugins::CommandLoader {
          protected:
            CommandVector commands() const {
                CommandVector cmds;
                cmds.push_back(boost::make_shared<ExampleCommand>());
                return cmds;
            }
          public:
            const string &name() const {
                static const string n = "exampleplugin";
                return n;
            }
            const string &version() const {
                static const string v = "0.0.1-pre-";
                return v;
            }
        } exampleInterface;

    }

} // namespace mongo

extern "C" {

    __attribute__((visibility("default")))
    mongo::plugins::PluginInterface *TokuMX_Plugin__getInterface(void) {
        return &mongo::example::exampleInterface;
    }

}
