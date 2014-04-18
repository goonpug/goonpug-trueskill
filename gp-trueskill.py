#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2014 Astroman Technologies LLC
#
# This file is part of GoonPUG
#
# GoonPUG is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# GoonPUG is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with GoonPUG.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import division, unicode_literals

import sys
import argparse
import copy
import re
import sqlite3
import SocketServer

import srcds.events.generic as generic_events
import srcds.events.csgo as csgo_events
from srcds.objects import BasePlayer

import skills
from skills import GaussianRating, RatingFactory
from skills.numerics import Range
from skills.trueskill import TrueSkillGameInfo, TwoTeamTrueSkillCalculator


SIDE_T = 1
SIDE_CT = 2


class GpSkillCalculator(TwoTeamTrueSkillCalculator):

    def __init__(self):
        super(TwoTeamTrueSkillCalculator, self).__init__(
            Range.exactly(2),
            Range.at_least(1),
            allow_partial_play=True,
            allow_partial_update=True
        )
        RatingFactory.rating_class = GaussianRating


class GoonPugPlayer(BasePlayer):
    pass


class GoonPugActionEvent(generic_events.BaseEvent):

    """GoonPUG triggered action event"""

    regex = ''.join([
        generic_events.BaseEvent.regex,
        r'GoonPUG triggered "(?P<action>.*?)"',
    ])

    def __init__(self, timestamp, action):
        super(GoonPugActionEvent, self).__init__(timestamp)
        self.action = action

    def __unicode__(self):
        msg = 'GoonPUG triggered "%s"' % (self.action)
        return ' '.join([super(GoonPugActionEvent, self).__unicode__(), msg])


class GoonPugParser(object):

    """GoonPUG log parser class"""

    def __init__(self, db, verbose=False):
        self.event_handlers = {
            generic_events.LogFileEvent: self.handle_log_file,
            generic_events.EnterGameEvent: self.handle_enter_game,
            generic_events.ConnectionEvent: self.handle_connection,
            generic_events.DisconnectionEvent: self.handle_disconnection,
            generic_events.KickEvent: self.handle_kick,
            generic_events.TeamActionEvent: self.handle_team_action,
            generic_events.WorldActionEvent: self.handle_world_action,
            generic_events.RoundEndTeamEvent: self.handle_round_end_team,
            csgo_events.SwitchTeamEvent: self.handle_switch_team,
            GoonPugActionEvent: self.handle_goonpug_action,
        }
        self.seen_players = {}
        self._compile_regexes()
        self._reset_matches()
        self.verbose = verbose
        self.conn = sqlite3.connect(db)
        c = self.conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS gpskill (
                steam_id TEXT PRIMARY KEY,
                rating REAL,
                variance REAL
            )
            ''')
        self.conn.commit()

    def _reset_matches(self):
        self.matches = []
        self._reset_current_match()

    def _reset_current_match(self):
        self.current_match = {
            'match_maps': [],
        }
        self._reset_current_match_map()

    def _reset_current_match_map(self):
        self.current_match_map = {
            'team_1': {},
            'team_2': {},
            'score_1': 0,
            'score_2': 0,
            'period': 0,
        }

    def _compile_regexes(self):
        """Add event types"""
        self.event_types = []
        for cls in self.event_handlers.keys():
            regex = re.compile(cls.regex)
            self.event_types.append((regex, cls))

    def parse_line(self, line):
        """Parse a single log line"""
        line = line.strip()
        if self.verbose:
            print line
        for (regex, cls) in self.event_types:
            match = regex.match(line)
            if match:
                event = cls.from_re_match(match)
                handler = self.event_handlers[type(event)]
                handler(event)
                return

    def _start_match(self, timestamp):
        self._reset_current_match()
        self.current_match_map['score_1'] = 0
        self.current_match_map['score_2'] = 0

    def _end_match(self, event):
        match_map = copy.copy(self.current_match_map)
        self.current_match['match_maps'].append(match_map)
        for match_map in self.current_match['match_maps']:
            self.update_ratings(match_map)

    def _start_round(self):
        for steam_id in self.cts:
            if self.current_match_map['period'] % 2:
                if steam_id not in self.current_match_map['team_1']:
                    self.current_match_map['team_1'][steam_id] = 0
                self.current_match_map['team_1'][steam_id] += 1
            else:
                if steam_id not in self.current_match_map['team_2']:
                    self.current_match_map['team_2'][steam_id] = 0
                self.current_match_map['team_2'][steam_id] += 1
        for steam_id in self.ts:
            if self.current_match_map['period'] % 2:
                if steam_id not in self.current_match_map['team_2']:
                    self.current_match_map['team_2'][steam_id] = 0
                self.current_match_map['team_2'][steam_id] += 1
            else:
                if steam_id not in self.current_match_map['team_1']:
                    self.current_match_map['team_1'][steam_id] = 0
                self.current_match_map['team_1'][steam_id] += 1

    def _end_round(self, event):
        rounds_played = self.current_match_map['score_1'] + \
            self.current_match_map['score_2']
        if rounds_played == 0:
            self.current_match_map['period'] = 1
        elif rounds_played < 30 and (rounds_played % 15) == 0:
            self.current_match_map['period'] += 1
        elif rounds_played >= 30 and (rounds_played % 6) == 0:
            self.current_match_map['period'] += 1

    def _sfui_notice(self, winning_team):
        pass

    def handle_log_file(self, event):
        if event.started:
            self.ts = set()
            self.cts = set()
            pattern = (r'.*L(?P<ip>\d+_\d+_\d+_\d+)_'
                       r'(?P<port>\d+)_(?P<time>\d+)_000.log')
            m = re.match(pattern, event.filename)
            if m:
                # TODO: If we support anything besides pugs this shouldn't be
                # reset
                self.match = {}
                ip = m.group('ip').replace('_', '.')
                port = int(m.group('port'))
                self.current_server = {'ip': ip, 'port': port}

    def handle_connection(self, event):
        steam_id = unicode(event.player.steam_id)
        if steam_id not in self.seen_players:
            self.seen_players[steam_id] = {}
        if event.address:
            self.seen_players[steam_id]['ip'] = event.address[0]

    def handle_enter_game(self, event):
        steam_id = unicode(event.player.steam_id)
        if steam_id not in self.seen_players:
            self.seen_players[steam_id] = {}
        self.seen_players[steam_id]['name'] = event.player.name

    def handle_disconnection(self, event):
        pass

    def handle_kick(self, event):
        # the leaving part should be taken care of by handle_disconnection
        pass

    def handle_team_action(self, event):
        if event.action == u"SFUI_Notice_Bomb_Defused":
            self._sfui_notice(event.team)
        elif event.action == u"SFUI_Notice_Target_Bombed":
            self._sfui_notice(event.team)
        elif event.action == u"SFUI_Notice_Target_Saved":
            self._sfui_notice(event.team)
        elif event.action == u"SFUI_Notice_Terrorists_Win" \
                or event.action == u"SFUI_Notice_CTs_Win":
            self._sfui_notice(event.team)

    def handle_world_action(self, event):
        if event.action == u'Round_Start':
            self._start_round()
        elif event.action == u'Round_End':
            self._end_round(event)

    def handle_goonpug_action(self, event):
        if event.action == u'Start_Match':
            self._start_match(event.timestamp)
        elif event.action in ['End_Match']:
            self._end_match(event)

    def handle_round_end_team(self, event):
        if 'period' not in self.current_match_map:
            self.current_match_map['period'] = 0
        if event.team == u'CT':
            if self.current_match_map['period'] % 2:
                self.current_match_map['score_1'] = event.score
            else:
                self.current_match_map['score_2'] = event.score
        elif event.team == u'TERRORIST':
            if self.current_match_map['period'] % 2:
                self.current_match_map['score_2'] = event.score
            else:
                self.current_match_map['score_1'] = event.score

    def handle_switch_team(self, event):
        steam_id = unicode(event.player.steam_id)

        try:
            if event.orig_team == 'CT':
                self.cts.remove(steam_id)
            elif event.orig_team == 'TERRORIST':
                self.ts.remove(steam_id)
        except KeyError:
            pass

        if event.new_team == 'CT':
            self.cts.add(steam_id)
        elif event.new_team == 'TERRORIST':
            self.ts.add(steam_id)

    def update_ratings(self, match_map):
        total_rounds = match_map['score_1'] + match_map['score_2']
        if total_rounds < 16:
            #skip bad match
            return
        team_1 = self._ratings_team('team_1', match_map, total_rounds)
        team_2 = self._ratings_team('team_2', match_map, total_rounds)
        if match_map['score_1'] > match_map['score_2']:
            rank = [1, 2]
        elif match_map['score_1'] < match_map['score_2']:
            rank = [2, 1]
        else:
            rank = [1, 1]
        teams = skills.Match([team_1, team_2], rank=rank)
        calc = GpSkillCalculator()
        game_info = TrueSkillGameInfo()
        new_ratings = calc.new_ratings(teams, game_info)
        for team in [match_map['team_1'], match_map['team_2']]:
            for steam_id in team:
                rating = new_ratings.rating_by_id(steam_id)
                self._update_player_skill(steam_id, rating.mean, rating.stdev)

    def _ratings_team(self, team_name, match_map, total_rounds):
        team = []
        for steam_id in match_map[team_name]:
            rounds_played = match_map[team_name][steam_id]
            partial_play = rounds_played / total_rounds
            if partial_play < 0.0001:
                partial_play = 0.0001
            if partial_play > 1.0:
                partial_play = 1.0
            (rating, variance) = self._fetch_player_skill(steam_id)
            player = skills.Player(player_id=steam_id,
                                   partial_play_percentage=partial_play)
            rating = skills.GaussianRating(rating, variance)
            team.append((player, rating,))
        return team

    def _fetch_player_skill(self, steam_id):
        c = self.conn.cursor()
        c.execute('''
            SELECT rating, variance FROM gpskill WHERE steam_id = {0}
        '''.format('?'), (steam_id,))
        row = c.fetchone()
        if row:
            (rating, variance) = row
        else:
            rating = 25.0
            variance = 25.0 / 3
            c.execute('''
                INSERT INTO gpskill (steam_id, rating, variance)
                VALUES ({0}, {0}, {0})
                '''.format('?'), (steam_id, rating, variance))
            self.conn.commit()
        return (rating, variance)

    def _update_player_skill(self, steam_id, rating, variance):
        c = self.conn.cursor()
        c.execute('''
            UPDATE gpskill SET rating = {0}, variance = {0}
            WHERE steam_id = {0}
            '''.format('?'), (rating, variance, steam_id))
        print '%s: %.03f' % (steam_id, get_conservative_rating(rating,
                                                               variance))
        self.conn.commit()


def get_conservative_rating(rating, variance):
    conservative_rating = rating - 3 * variance
    if conservative_rating < 0.0:
        return 0.0
    else:
        return conservative_rating


log_parsers = {}


class GoonPugLogHandler(SocketServer.DatagramRequestHandler):

    verbose = False
    db = ''

    def handle(self):
        data = self.request[0]
        # Strip the 4-byte header and the first 'R' character
        #
        # There is no documentation for this but I am guessing the 'R' stands
        # for 'Remote'? Either way normal log entires are supposed to start
        # with 'L', but the UDP packets start with 'RL'
        data = data[5:].strip()
        if not self.client_address in log_parsers:
            print u'Got new connection from {}'.format(self.client_address[0])
            parser = GoonPugParser(GoonPugLogHandler.db,
                                   GoonPugLogHandler.verbose)
            log_parsers[self.client_address] = parser
        log_parsers[self.client_address].parse_line(data)


def main():
    parser = argparse.ArgumentParser(description='GoonPUG logparser')
    parser.add_argument('-p', '--port', dest='port', action='store', type=int,
                        default=27500, help='port to listen on')
    parser.add_argument('-s', action='store_true', dest='stdin',
                        help='read log entries from stdin instead of '
                        'listening on a network port')
    parser.add_argument('-v', action='store_true', dest='verbose',
                        help='verbose output')
    parser.add_argument('db', metavar='DATABASE', help='path to the sqlite db')
    args = parser.parse_args()
    if args.stdin:
        log_parser = GoonPugParser(args.db, verbose=args.verbose)
        print "goonpugd: Reading from STDIN"
        while True:
            try:
                for line in sys.stdin.readlines():
                    log_parser.parse_line(line)
            except KeyboardInterrupt:
                sys.exit()
            except EOFError:
                sys.exit()
    else:
        print 'goonpugd: Listening for HL log connections on port %d' % (
            args.port)
        GoonPugLogHandler.verbose = args.verbose
        GoonPugLogHandler.db = args.db
        server = SocketServer.UDPServer(('0.0.0.0', args.port),
                                        GoonPugLogHandler)
        server.timeout = 30
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print 'goonpugd: exiting'
            sys.exit()


if __name__ == '__main__':
    main()
