## Licensed under the Apache License, Version 2.0 (the "License"); you may not
## use this file except in compliance with the License. You may obtain a copy of
## the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
## License for the specific language governing permissions and limitations under
## the License.

couchlibdir = $(localerlanglibdir)/couch-$(version)
couchincludedir = $(couchlibdir)/include

couchpcdir = $(pkgconfigdir)

if PKG_CONFIG
couchpc_DATA = couchdb.pc
CLEANFILES = couchdb.pc
endif

couchdb.pc: couchdb.pc.tpl
	sed -e "s|%prefix%|@prefix@|g" \
			-e "s|%couchincludedir%|${couchincludedir}|g" \
			-e "s|%localerlanglibdir%|@localerlanglibdir@|g" > \
	$@ < $<

