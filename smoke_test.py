from pathlib import Path
from collections import defaultdict

from kosmobot.config import Settings
from kosmobot.db import Database, UserProfile
from kosmobot.excel_sync import ExcelRepository
from kosmobot.bot import BotServices, build_application


def main() -> None:
    base = Path(__file__).resolve().parent
    excel_path = base / 'КосмоФентези_v4_BeteraPool_complete.xlsx'
    excel = ExcelRepository(excel_path)
    participants = excel.load_participants()
    rosters = excel.load_rosters()
    assert participants, 'participants not loaded'
    assert rosters, 'rosters not loaded'

    db_path = base / 'smoke.sqlite3'
    if db_path.exists():
        db_path.unlink()
    db = Database(db_path)
    settings = Settings(
        bot_token='123:ABC',
        admin_ids={1},
        excel_path=excel_path,
        db_path=db_path,
        league_name='КосмоФентези',
        timezone='Europe/Amsterdam',
        lineup_deadline_hour=18,
        lineup_deadline_minute=0,
        current_stage='Регулярка',
        current_round=1,
        allow_self_scoring=False,
        default_lineup_limits='Вратарь=1,Защитник=2,Нападающий=3',
    )
    services = BotServices(settings=settings, db=db, excel=excel)
    _ = build_application(services)

    participant = participants[0]
    db.upsert_user(UserProfile(telegram_id=1, username='admin', full_name='Admin', participant_name=participant, is_admin=True))
    players = rosters[participant]

    selected = []
    need = {'Вратарь': 1, 'Защитник': 2, 'Нападающий': 3}
    used_skater_teams = set()
    for pl in players:
        pos = pl.position.lower()
        if 'врат' in pos:
            bucket = 'Вратарь'
        elif 'защ' in pos:
            bucket = 'Защитник'
        else:
            bucket = 'Нападающий'
        if need[bucket] <= 0:
            continue
        if bucket != 'Вратарь' and pl.team_name in used_skater_teams:
            continue
        selected.append((pl.name, pl.position))
        need[bucket] -= 1
        if bucket != 'Вратарь':
            used_skater_teams.add(pl.team_name)
        if sum(need.values()) == 0:
            break
    assert sum(need.values()) == 0, f'could not build lineup: {need}'
    db.replace_lineup(participant, 'Регулярка', 1, selected, 1)
    assert len(db.get_effective_lineup(participant, 'Регулярка', 1)) == 5

    team_of = {p.name: p.team_name for p in players}
    pos_of = {p.name: p.position for p in players}
    by_pos = defaultdict(list)
    for p in players:
        by_pos[p.position].append(p)

    reg2 = list(selected)
    current_names = {n for n, _ in reg2}
    current_skater_teams = {team_of[n] for n, pos in reg2 if 'врат' not in pos.lower()}
    old_name = None
    new_name = None
    for idx, (name, pos) in enumerate(reg2):
        if 'защ' not in pos.lower():
            continue
        old_name = name
        old_team = team_of[name]
        for cand in by_pos[pos]:
            if cand.name in current_names:
                continue
            if cand.team_name in current_skater_teams and cand.team_name != old_team:
                continue
            reg2[idx] = (cand.name, cand.position)
            new_name = cand.name
            break
        if new_name:
            break
    assert old_name and new_name, 'regular transfer candidate not found'
    db.replace_lineup(participant, 'Регулярка', 2, reg2, 1)
    db.replace_transfers(participant, 'Регулярка', 2, [(old_name, new_name)], 1, 'regular')
    assert db.count_regular_transfers_used(participant) == 1

    po1 = list(reg2)
    current_names = {n for n, _ in po1}
    current_skater_teams = {team_of[n] for n, pos in po1 if 'врат' not in pos.lower()}
    changes = []
    for idx, (name, pos) in enumerate(list(po1)):
        if 'врат' in pos.lower():
            continue
        old_team = team_of[name]
        for cand in by_pos[pos]:
            if cand.name in current_names:
                continue
            if cand.team_name in current_skater_teams and cand.team_name != old_team:
                continue
            po1[idx] = (cand.name, cand.position)
            current_names.remove(name)
            current_names.add(cand.name)
            current_skater_teams.remove(old_team)
            current_skater_teams.add(cand.team_name)
            changes.append((name, cand.name))
            break
        if len(changes) == 2:
            break
    assert len(changes) == 2, 'playoff transfer candidates not found'
    db.replace_lineup(participant, 'Плей-офф', 1, po1, 1)
    db.replace_transfers(participant, 'Плей-офф', 1, changes, 1, 'playoff')
    assert len(db.get_transfers(participant, 'Плей-офф', 1)) == 2

    player_name, position = po1[0]
    db.upsert_score(participant, 'Плей-офф', 1, player_name, position, 0, 0, 0, True, 0.0, True, 3.0, 1)
    assert len(db.get_scores(participant, 'Плей-офф', 1)) == 1
    out = excel.export_bot_data(db.list_users(), db.all_lineups(), db.get_scores(), db.leaderboard(), base / 'smoke_export.xlsx', db.get_transfers())
    assert out.exists(), 'export file not created'
    print('OK: participants=%d pool=%d users=%d lineup=%d transfers_po=%d scores=%d' % (
        len(participants), len(excel.load_global_pool()), len(db.list_users()), len(db.get_effective_lineup(participant, 'Плей-офф', 1)), len(db.get_transfers(participant, 'Плей-офф', 1)), len(db.get_scores()),
    ))


if __name__ == '__main__':
    main()
