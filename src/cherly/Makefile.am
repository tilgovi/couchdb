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

SUBDIRS = priv

cherlyebindir = $(localerlanglibdir)/cherly-d0bd62/ebin
cherlybindir = $(cherlylibdir)/ebin

source_files = \
    cherly.app.in \
    cherly_app.erl \
    cherly.erl

compiled_files = \
    cherly.app \
    cherly_app.beam \
    cherly.beam

cherlyebin_DATA = \
    $(compiled_files)

EXTRA_DIST =  \
    $(source_files)

CLEANFILES = \
    $(compiled_files)

%.app: %.app.in
	cp $< $@

%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<
