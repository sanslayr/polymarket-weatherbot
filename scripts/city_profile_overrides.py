"""Curated city-specific notes for L2 station profile generation."""

from __future__ import annotations


CITY_CLIMATE_WINDOWS: dict[str, list[dict[str, object]]] = {
    "CYYZ": [
        {"label": "冬季(12-2)", "months": [12, 1, 2]},
        {"label": "春季(3-5)", "months": [3, 4, 5]},
        {"label": "夏季(6-8)", "months": [6, 7, 8]},
        {"label": "秋季(9-11)", "months": [9, 10, 11]},
    ],
    "EDDM": [
        {"label": "冬季(12-2)", "months": [12, 1, 2]},
        {"label": "春季(3-5)", "months": [3, 4, 5]},
        {"label": "夏季(6-8)", "months": [6, 7, 8]},
        {"label": "秋季(9-11)", "months": [9, 10, 11]},
    ],
    "EGLC": [
        {"label": "冷季(11-2)", "months": [11, 12, 1, 2]},
        {"label": "春季(3-5)", "months": [3, 4, 5]},
        {"label": "暖季(6-8)", "months": [6, 7, 8]},
        {"label": "秋季(9-10)", "months": [9, 10]},
    ],
    "KATL": [
        {"label": "冷季(12-2)", "months": [12, 1, 2]},
        {"label": "春季(3-5)", "months": [3, 4, 5]},
        {"label": "暖湿季(6-8)", "months": [6, 7, 8]},
        {"label": "秋季(9-11)", "months": [9, 10, 11]},
    ],
    "KDAL": [
        {"label": "冷季(12-2)", "months": [12, 1, 2]},
        {"label": "春季强对流季(3-5)", "months": [3, 4, 5]},
        {"label": "盛夏热季(6-9)", "months": [6, 7, 8, 9]},
        {"label": "秋季(10-11)", "months": [10, 11]},
    ],
    "KLGA": [
        {"label": "冬季(12-2)", "months": [12, 1, 2]},
        {"label": "春季(3-5)", "months": [3, 4, 5]},
        {"label": "夏季(6-8)", "months": [6, 7, 8]},
        {"label": "秋季(9-11)", "months": [9, 10, 11]},
    ],
    "KMIA": [
        {"label": "干季(11-4)", "months": [11, 12, 1, 2, 3, 4]},
        {"label": "雨季(5-10)", "months": [5, 6, 7, 8, 9, 10]},
    ],
    "KORD": [
        {"label": "冬季(12-2)", "months": [12, 1, 2]},
        {"label": "春季(3-5)", "months": [3, 4, 5]},
        {"label": "夏季(6-8)", "months": [6, 7, 8]},
        {"label": "秋季(9-11)", "months": [9, 10, 11]},
    ],
    "KSEA": [
        {"label": "湿凉季(11-3)", "months": [11, 12, 1, 2, 3]},
        {"label": "转季(4-6)", "months": [4, 5, 6]},
        {"label": "干暖季(7-9)", "months": [7, 8, 9]},
        {"label": "回湿季(10)", "months": [10]},
    ],
    "LFPG": [
        {"label": "冬季(12-2)", "months": [12, 1, 2]},
        {"label": "春季(3-5)", "months": [3, 4, 5]},
        {"label": "夏季(6-8)", "months": [6, 7, 8]},
        {"label": "秋季(9-11)", "months": [9, 10, 11]},
    ],
    "LTAC": [
        {"label": "冬季(12-2)", "months": [12, 1, 2]},
        {"label": "春季(3-5)", "months": [3, 4, 5]},
        {"label": "夏季(6-8)", "months": [6, 7, 8]},
        {"label": "秋季(9-11)", "months": [9, 10, 11]},
    ],
    "NZWN": [
        {"label": "暖季(11-3)", "months": [11, 12, 1, 2, 3]},
        {"label": "转季(4-5,10)", "months": [4, 5, 10]},
        {"label": "冷季(6-9)", "months": [6, 7, 8, 9]},
    ],
    "RKSI": [
        {"label": "冬季(12-2)", "months": [12, 1, 2]},
        {"label": "春季(3-5)", "months": [3, 4, 5]},
        {"label": "暖湿季(6-8)", "months": [6, 7, 8]},
        {"label": "秋季(9-11)", "months": [9, 10, 11]},
    ],
    "SAEZ": [
        {"label": "暖季(12-2)", "months": [12, 1, 2]},
        {"label": "秋季(3-5)", "months": [3, 4, 5]},
        {"label": "冷季(6-8)", "months": [6, 7, 8]},
        {"label": "春季(9-11)", "months": [9, 10, 11]},
    ],
    "SBGR": [
        {"label": "雨季(10-3)", "months": [10, 11, 12, 1, 2, 3]},
        {"label": "转季(4-5)", "months": [4, 5]},
        {"label": "干季(6-9)", "months": [6, 7, 8, 9]},
    ],
    "VILK": [
        {"label": "冷季 / 雾季(11-2)", "months": [11, 12, 1, 2]},
        {"label": "前季风热季(3-5)", "months": [3, 4, 5]},
        {"label": "雨季 / 季风季(6-9)", "months": [6, 7, 8, 9]},
        {"label": "后季风(10)", "months": [10]},
    ],
}


CITY_PROFILE_OVERRIDES: dict[str, dict[str, object]] = {
    "CYYZ": {
        "core_identity": "湖滨缓丘城郊机场，温度更接近“湖陆过渡场”而不是多伦多主城区热岛本体。",
        "decisive_factors": [
            "南到东南风最容易把暖湿空气直接送进机场样本，北西向更常对应冷平流或后侧干冷背景。",
            "冬半年低云和能见度限制偏重，午前能否抬云开窗比单报温度更有解释力。",
            "晚峰比例高，若午后仍在升温，不应按普通早峰站点提前封顶。",
        ],
        "failure_modes": [
            "把 CYYZ 当作 downtown Toronto 热岛代理，会系统性高估暖季上限。",
            "忽略湖两侧代表性差异，拿 Buffalo 一类剖面替代，会把近地层结构看偏。",
        ],
        "watch_order": [
            "先看地面风是否落在 S/SE 暖湿通道，再看午前低云能否抬升，最后看雨后地面恢复速度。",
            "若 15L 后仍保有升温斜率，应保留尾段再冲一档的空间。",
        ],
        "repo_notes": [
            "探空硬规则：Toronto 禁用 Buffalo 剖面，湖两侧代表性不一致。",
        ],
        "focus_slices": [
            {
                "label": "暖季暖通道分风向表现",
                "months": [6, 7, 8],
                "conditions": {"midday_low_ceiling_flag": False, "precip_day_flag": False},
                "sectors": ["S", "SE", "W", "NW"],
                "min_days": 40,
                "min_sector_days": 15,
                "note": "用来区分湖风/冷平流背景与真正暖湿通道，不要把 Toronto 热岛经验直接套到 CYYZ。",
            }
        ],
    },
    "EDDM": {
        "core_identity": "山前平原机场，峰值是否后移通常取决于晨雾/低云持续多久，以及午前是否出现真正开窗。",
        "decisive_factors": [
            "低云和能见度受限频率很高，晨间层结一旦拖到中午，全天上限会被直接压平。",
            "一旦午前开窗成功，慕尼黑并不缺升温能力，尾段继续上冲的概率明显高于直觉。",
            "西风常见但不总是最暖，真正高上限更多出现在更干净、更少低云的象限组合。",
        ],
        "failure_modes": [
            "看到上午偏冷就直接判全天偏冷，容易错过开窗后的追赶段。",
            "只用单报温度判断已封顶，会漏掉 16-18L 的迟到峰值。",
        ],
        "watch_order": [
            "先看 FG/BR 与低云云底是否抬升，再看午前斜率是否恢复，最后再判断峰值窗是否被后移。",
            "若 14L 后仍在提速，优先防旧版过早封顶。",
        ],
        "repo_notes": [
            "既有调研：EDDM 的晨雾/低云持续时长和开窗时点，比单报温度更关键。",
        ],
        "focus_slices": [
            {
                "label": "暖季开窗日分风向表现",
                "months": [5, 6, 7, 8, 9],
                "conditions": {
                    "midday_low_ceiling_flag": False,
                    "precip_day_flag": False,
                },
                "sectors": ["E", "SE", "W", "NW", "N"],
                "min_days": 70,
                "min_sector_days": 12,
                "note": "用于拆开慕尼黑暖季开窗成功后的风向差异，重点看晚峰是否后移，以及哪些象限更容易保留尾段上冲。",
            }
        ],
    },
    "EGLC": {
        "core_identity": "河口低地城市站，温度振幅偏小，云量和河口/海洋调制经常压过纯辐射升温逻辑。",
        "decisive_factors": [
            "河口近水体背景让低云持续性非常关键，云不散时很难走出典型内陆日较差。",
            "偏东风样本更容易对应较暖上限，西南风常见但更容易叠加海洋性云层和抑制。",
            "即便属于晚峰站，尾段冲高也通常建立在先成功开窗的前提上。",
        ],
        "failure_modes": [
            "把 London City 直接当作巴黎式内陆机场，会高估晴空上冲的普适性。",
            "忽略云层持续性，只看当前温度斜率，容易把“被压制但未开窗”和“真封顶”混为一类。",
        ],
        "watch_order": [
            "先看峰值窗低云是否仍压在场上，再看风向是偏东暖化还是西南海洋性抑制，最后看能见度和湿层信号。",
        ],
        "focus_slices": [
            {
                "label": "暖季开窗后分风向表现",
                "months": [5, 6, 7, 8, 9],
                "conditions": {
                    "midday_low_ceiling_flag": False,
                    "precip_day_flag": False,
                },
                "sectors": ["E", "SE", "SW", "W", "NW"],
                "min_days": 60,
                "min_sector_days": 10,
                "note": "用于区分河口站在偏东暖化与偏西南海洋性背景下的上限差异，避免把 London City 当成纯内陆站。",
            }
        ],
    },
    "KATL": {
        "core_identity": "内陆高地湿热站，晴空干混合上冲和降水重置压制都很强，必须把两条路径分开看。",
        "decisive_factors": [
            "西南到西风是更稳的暖上限通道，配合少云时容易把 Tmax 顶得更高。",
            "雨后地面湿润和残余云团会明显拖慢午前启动，热季尤其不能忽视重置效应。",
            "湿热滞留和干混合都不低，说明同样气温起点下，露点背景会决定后续斜率能否继续抬升。",
        ],
        "failure_modes": [
            "把所有暖季日都按干热清空日外推，会高估阵雨残留日。",
            "把午前偏慢直接视为全天失败，也会漏掉开窗后再追高的情况。",
        ],
        "watch_order": [
            "先排查早段是否有降水重置，再看云层残留和露点，最后看 SW/W 暖通道是否建立。",
        ],
        "focus_slices": [
            {
                "label": "暖季晴热日分风向表现",
                "months": [5, 6, 7, 8, 9],
                "conditions": {
                    "midday_low_ceiling_flag": False,
                    "precip_day_flag": False,
                },
                "sectors": ["SW", "W", "NW", "E", "NE"],
                "min_days": 90,
                "min_sector_days": 15,
                "note": "用于拆开 Atlanta 暖区西南风、干混合西风与偏东侧相对保守样本，重点看 Tmax 和尾段斜率。",
            }
        ],
    },
    "KDAL": {
        "core_identity": "内陆平原热站，清空辐射升温和暖区南风的叠加非常凶，旧版最容易在尾段低估。",
        "decisive_factors": [
            "南风是最稳的高温通道，东南/东风也常保有较高热量背景。",
            "春季和梅雨前后的云盾/降水残留会显著改变启动速度，必须和典型晴热日分开。",
            "清空日和 clean-solar-ramp 占比高，说明一旦上午没有被压制，下午上冲空间通常还在。",
        ],
        "failure_modes": [
            "午后稍微横盘就封顶，容易漏掉 16-17L 的二次抬升。",
            "忽视北风/西北风冷平流和出流边界，会把本该降档的日子看得过热。",
        ],
        "watch_order": [
            "先判定是否在暖区南风里，再看春季云盾/降水残留，最后看 14-17L 是否仍保留 clean ramp。",
        ],
        "focus_slices": [
            {
                "label": "暖季晴空增温日分风向表现",
                "months": [5, 6, 7, 8, 9],
                "conditions": {
                    "clean_solar_ramp_flag": True,
                    "precip_day_flag": False,
                },
                "sectors": ["S", "SE", "E", "W", "NW"],
                "min_days": 90,
                "min_sector_days": 12,
                "note": "用于区分 Dallas 暖区南风和边界后侧偏西北风背景，避免把所有清空热日都按同一上冲路径处理。",
            }
        ],
    },
    "KLGA": {
        "core_identity": "湾岸低地机场，海陆气团切换速度快，站点表现不等同于纽约主城区热岛。",
        "decisive_factors": [
            "南风是最稳的暖上限样本，西风和北风更容易把机场样本拉回偏凉或偏内陆的轨道。",
            "雨后重置和低云在暖季都不少，说明午前恢复速度比静态 climatology 更重要。",
            "晚峰虽然存在，但整体达峰比真正内陆晚峰站更早，不能机械套用 17L 以后仍强冲的模板。",
        ],
        "failure_modes": [
            "把 LaGuardia 直接视作 Manhattan 热岛，会高估日内上限和晚峰幅度。",
            "忽略湾岸海风/海洋性云的入侵时点，会把后段温度路径看得过于平滑。",
        ],
        "watch_order": [
            "先看地面风是南向暖化还是西北向偏凉，再看峰值窗低云与降水残余，最后看是否还有晚段追高迹象。",
        ],
        "focus_slices": [
            {
                "label": "暖季海风 / 内陆风分型",
                "months": [5, 6, 7, 8, 9],
                "conditions": {
                    "midday_low_ceiling_flag": False,
                },
                "sectors": ["S", "SW", "W", "NW", "E"],
                "min_days": 70,
                "min_sector_days": 12,
                "note": "用于拆开 LaGuardia 在南向暖湿、偏西内陆和偏北后侧样本下的表现，避免直接套 Manhattan 热岛经验。",
            }
        ],
    },
    "KMIA": {
        "core_identity": "滨海湿地热带站，峰值偏早，海风、露点和午后对流比单纯太阳辐射更决定上限。",
        "decisive_factors": [
            "高露点是底色，真正决定能否冲得更高的是海风切入时间和午前云量演变。",
            "东到东南风是常态，但最热样本更偏向南到西南的暖湿通道。",
            "雨后重置频率高，说明同样起报温度下，积云发展和阵雨残留会快速改写上限。",
        ],
        "failure_modes": [
            "把 Miami 当成干热内陆站，会高估尾段冲高并低估午后回落。",
            "只看气温不看露点与风向，容易把“海风锁温”误判成模型冷偏差。",
        ],
        "watch_order": [
            "先看早晨露点和云底，再看风向是否转入海风/西南暖通道，最后看午后对流是否开始侵入峰值窗。",
        ],
        "focus_slices": [
            {
                "label": "暖季高湿日分风向表现",
                "months": [5, 6, 7, 8, 9],
                "conditions": {"humid_sticky_flag": True},
                "sectors": ["SE", "E", "S", "SW", "NW"],
                "min_days": 60,
                "min_sector_days": 12,
                "note": "关注海风锁温与西南暖通道的差异，午后对流会快速改写后段上限。",
            }
        ],
    },
    "KORD": {
        "core_identity": "城市西北远郊机场，气温更多受锋面后风向和云量控制，不应直接等同市中心热岛。",
        "decisive_factors": [
            "南到西南风最容易提供高上限，西北到西风更常对应后侧偏冷或混合不足样本。",
            "降水重置和低云压制都不低，说明前一轮对流或锋前/锋后残留很关键。",
            "虽有晚峰倾向，但整体更像前沿风向主导站，而不是纯地面热岛型站点。",
        ],
        "failure_modes": [
            "按 downtown Chicago 的热岛路径外推，会高估 O'Hare 暖季峰值。",
            "忽略 frontal wind shift，只看同小时温度，会把后侧冷平流日误判成暂时偏冷。",
        ],
        "watch_order": [
            "先看风是否留在 S/SW 暖区，再看云/降水残留，最后评估午后还能否保持升温斜率。",
        ],
        "focus_slices": [
            {
                "label": "暖季锋前 / 锋后分风向表现",
                "months": [5, 6, 7, 8, 9],
                "conditions": {
                    "precip_day_flag": False,
                },
                "sectors": ["S", "SW", "W", "NW", "N"],
                "min_days": 90,
                "min_sector_days": 15,
                "note": "用于区分 O'Hare 在暖区南风与锋后西北风样本下的 Tmax 和峰值窗，避免把郊区机场按 downtown Chicago 外推。",
            }
        ],
    },
    "KSEA": {
        "core_identity": "海陆过渡站，海洋层云和降水重置是常态压制，但晴空日仍可能走出明显晚峰。",
        "decisive_factors": [
            "低云和雨后重置频率高，午前是否成功开窗决定了当天是否仍有追高空间。",
            "夏季 clean ramp 和干混合上冲并不少，说明不要把 Seattle 机械视作全天平缓的海洋站。",
            "常态样本里东到东南风多对应冷季或阴湿背景，不能把年平均风向效应直接硬套到极端热浪日。",
        ],
        "failure_modes": [
            "上午偏冷就直接判全天失败，会漏掉开窗后的迟到峰值。",
            "反过来若 marine layer 未散却按热浪日模板外推，也会系统性高估。",
        ],
        "watch_order": [
            "先看低云能否在中午前明显退场，再看雨后地面是否恢复，最后看 15-18L 是否进入晚峰轨道。",
        ],
        "focus_slices": [
            {
                "label": "暖季开窗日分风向表现",
                "months": [6, 7, 8, 9],
                "conditions": {
                    "midday_low_ceiling_flag": False,
                    "precip_day_flag": False,
                },
                "sectors": ["N", "NE", "W", "SW", "S"],
                "min_days": 60,
                "min_sector_days": 10,
                "note": "用于拆开 Seattle marine layer 退场后的不同风向路径，重点看哪些样本会转入真正的晚峰上冲。",
            }
        ],
    },
    "LFPG": {
        "core_identity": "盆地平原晚峰站，真正的危险不是晨间偏低，而是 16-18L 仍未结束的尾段升温。",
        "decisive_factors": [
            "晚峰比例极高，说明巴黎机场样本不能用传统 14-15L 封顶逻辑处理。",
            "东北到西北风更容易对应较高上限，西南风更常叠加海洋性抑制和较低温顶。",
            "低云压制并不罕见，但只要午前后成功开窗，后续补涨能力非常强。",
        ],
        "failure_modes": [
            "把 LFPG 当作普通欧洲平原日变化，会系统性低估后段峰值。",
            "看到 14L 暂时横盘就封顶，常常错过 17L 左右的真正高点。",
        ],
        "watch_order": [
            "先判断风向是否偏 continental，再看午前云量能否削弱，最后盯 15-18L 是否仍保持上冲结构。",
        ],
        "focus_slices": [
            {
                "label": "暖季大陆性 / 海洋性分风向表现",
                "months": [5, 6, 7, 8, 9],
                "conditions": {
                    "precip_day_flag": False,
                },
                "sectors": ["NE", "E", "NW", "W", "SW"],
                "min_days": 90,
                "min_sector_days": 15,
                "note": "用于拆开 Paris 机场在 continental 与 marine 象限下的晚峰强度差异，避免 14-15L 过早封顶。",
            }
        ],
    },
    "LTAC": {
        "core_identity": "内陆高原机场，大日较差和清空混合是底色，峰值更像被云量和风向切换决定。",
        "decisive_factors": [
            "风向热力效应有明显季节差异：冬季更暖样本并不固定落在 NE，春季与夏季的暖象限也不完全相同，不能用全年平均一句话概括。",
            "高原干混合和 clean-solar-ramp 占比高，少云日很容易把温度一路抬到后段。",
            "春季云量和降水残留仍有不小影响，不能把所有高原日都当作纯干热日。",
        ],
        "failure_modes": [
            "在晴空高原日过早封顶，会明显低估后段冲高。",
            "把风向影响全年静态化，会错过 2-3 月过渡季和盛夏样本里完全不同的暖冷分布。",
        ],
        "watch_order": [
            "先看月份与季节背景，再看云量和近地层干燥度，随后判断当日风向是否落在该季节的暖象限，最后看 15-17L 是否仍在上冲。",
        ],
        "focus_slices": [
            {
                "label": "冬季晴日分风向表现",
                "months": [12, 1, 2],
                "conditions": {
                    "midday_low_ceiling_flag": False,
                    "precip_day_flag": False,
                    "reduced_visibility_flag": False,
                },
                "sectors": ["NE", "N", "SW", "S", "W"],
                "min_days": 80,
                "min_sector_days": 15,
                "note": "用于拆开高原冬季晴日里不同风向对应的 Tmax / Tmin / 日较差，避免把所有冬晴日看成同一种路径。",
            },
            {
                "label": "夏季干混合日分风向表现",
                "months": [6, 7, 8],
                "conditions": {
                    "dry_mixing_flag": True,
                    "midday_low_ceiling_flag": False,
                    "precip_day_flag": False,
                },
                "sectors": ["NE", "N", "E", "SW", "W"],
                "min_days": 45,
                "min_sector_days": 10,
                "note": "用于拆开 Ankara 夏季高原干混合背景下的暖通道和偏冷风向，重点看 Tmax、日较差和峰值是否继续后移。",
            }
        ],
    },
    "NZWN": {
        "core_identity": "海湾丘陵强风口站点，温度经常由风向/风力与云量联合作用决定，远强于普通辐射升温曲线。",
        "decisive_factors": [
            "北向样本显著更暖，南到东南象限更容易把温度锁在更低平台。",
            "午间低云一旦不退，午前升温会直接被压平；Wellington 往往不是“能不能热”，而是“风和云给不给窗口”。",
            "风向切换占比很高，北-南象限切换时应把实况当成 live pivot，而不是只看模式偏差。",
        ],
        "failure_modes": [
            "把 Wellington 按普通沿海站点的平滑日变化外推，容易完全错过风向转折后的路径改写。",
            "只看温度不看风速/风向和云量，会高估尾段冲高概率。",
        ],
        "watch_order": [
            "先看风向是否在北-南象限切换，再看平均风/阵风级别，最后看峰值窗云量与云底。",
            "若北向较暖流建立且云量快速减弱，再考虑尾段追高；否则优先防被云和风锁温。",
        ],
        "focus_slices": [
            {
                "label": "暖季北/南风主导日表现",
                "months": [1, 2, 3, 11, 12],
                "conditions": {"midday_low_ceiling_flag": False},
                "sectors": ["N", "S", "SE"],
                "min_days": 50,
                "min_sector_days": 10,
                "note": "用于直接比较 Wellington 北向暖流、南向锁温和东南偏冷样本的温度与日较差表现。",
            }
        ],
    },
    "RKSI": {
        "core_identity": "填海沿海机场，必须把它当成 Incheon 海湾站而不是首尔城区热岛站来理解。",
        "decisive_factors": [
            "南到西南风样本更暖，北到西北风更常对应更冷的海湾/后侧背景。",
            "湿热持续性很高，说明高露点和海风/近海水体对上限约束比纯干混合更关键。",
            "低云虽然不像 Wellington 那样一票否决，但会明显压缩日较差和午前升温效率。",
        ],
        "failure_modes": [
            "把 RKSI 当作内陆 Seoul 热岛代理，会系统性高估晴热上限。",
            "忽略 Incheon 近海下垫面，只用城市感知做修正，会把云和风向敏感度看轻。",
        ],
        "watch_order": [
            "先看风向是否从 N/NW 转入 S/SW 暖通道，再看露点和低云，最后评估是否具备晚段追高条件。",
        ],
        "repo_notes": [
            "探空硬规则：Seoul 场景强制使用 Incheon(47113)；24h 内无实测则禁用实测探空。",
        ],
        "focus_slices": [
            {
                "label": "暖季沿海风向分型",
                "months": [6, 7, 8, 9],
                "conditions": {"humid_sticky_flag": True},
                "sectors": ["S", "SW", "W", "NW", "N"],
                "min_days": 50,
                "min_sector_days": 12,
                "note": "用来拆开沿海暖湿通道和偏北/偏西海湾抑制背景，不要把 RKSI 当成首尔城区热岛。",
            }
        ],
    },
    "SAEZ": {
        "core_identity": "河口平原大日较差站，北到东北暖平流与南侧冷空气重置切换非常关键。",
        "decisive_factors": [
            "N/NE/E 象限更容易给出高上限，西南/西风则更常是冷空气或河口海风后侧背景。",
            "晴空 clean ramp 和晚峰都不少，说明尾段仍可继续抬升，尤其在暖区维持时。",
            "云压制和降水重置不能忽略，但相比湿热站，更像是“阶段性压制”而不是整天闷住。",
        ],
        "failure_modes": [
            "忽略南风切入后的快速降档，会把暖区延续看过头。",
            "把 Buenos Aires 全部按海边平缓日变化处理，会低估内陆式大日较差。",
        ],
        "watch_order": [
            "先看是否维持在 N/NE 暖区，再看冷空气/南风何时推进，最后看云量是否会截断 16L 后的尾段上冲。",
        ],
        "focus_slices": [
            {
                "label": "暖季南北气团分风向表现",
                "months": [12, 1, 2],
                "conditions": {
                    "precip_day_flag": False,
                    "midday_low_ceiling_flag": False,
                },
                "sectors": ["N", "NE", "E", "S", "SW"],
                "min_days": 70,
                "min_sector_days": 12,
                "note": "用于拆开 Buenos Aires 河口平原在北向暖平流和南侧冷空气切入时的温顶差异，重点看日较差和峰值是否被截断。",
            }
        ],
    },
    "SBGR": {
        "core_identity": "山地高地机场，常年云量和湿度负担重，真正的高温窗口往往来自云量明显减弱而不是单纯背景偏暖。",
        "decisive_factors": [
            "低云压制是首要风险，云量持续时日较差和上冲空间都会被明显削弱。",
            "西北象限更容易出现暖峰样本，南到东南风则常对应更低温顶和更重抑制。",
            "湿热滞留占比不低，说明即使气温起点不差，也可能因高露点和云层导致升温效率不足。",
        ],
        "failure_modes": [
            "把 Sao Paulo 当作干热高原站，会系统性高估晴空上冲的出现频率。",
            "只看实时温度不看天顶云量/云底变化，会漏掉“全天被压着走”的样本。",
        ],
        "watch_order": [
            "先看午前低云是否抬升，再看风向是否从 S/SE 转向更暖象限，最后看露点是否仍高到足以拖累升温效率。",
        ],
        "focus_slices": [
            {
                "label": "暖季开窗后分风向表现",
                "months": [9, 10, 11, 12, 1, 2, 3],
                "conditions": {
                    "midday_low_ceiling_flag": False,
                },
                "sectors": ["NW", "W", "N", "S", "SE"],
                "min_days": 70,
                "min_sector_days": 12,
                "note": "用于拆开 Guarulhos 在云量减弱后不同风向下的升温能力，重点看哪些象限仍受高湿和低云拖累。",
            }
        ],
    },
    "VILK": {
        "core_identity": "恒河平原站点，晨间低能见/湿层散除时点与对流季触发风险，往往比单小时气温更决定最高温。",
        "decisive_factors": [
            "低能见和轻风停滞非常常见，早晨 BR/HZ/FG 何时散掉，直接决定午前能否进入有效升温。",
            "东到东南风样本更暖，说明暖湿平原气团和地面混合窗口的耦合很重要。",
            "前季风和季风季的云雨/对流会明显压缩日较差，即使日最高温仍然偏高，也更依赖触发前的抢跑。",
        ],
        "failure_modes": [
            "把 Lucknow 全年都按干热内陆站处理，会低估 haze/fog 与对流季截断的影响。",
            "只盯温度斜率而忽视能见度和露点差变化，容易错判是否已进入真正混合层增长。",
        ],
        "watch_order": [
            "先看 BR/HZ/FG 与能见度改善时点，再看露点差是否扩张，最后看午后对流触发风险是否切断峰值窗。",
        ],
        "repo_notes": [
            "既有调研：Lucknow 应提高晨间 BR/HZ 散除时点和对流触发温度的权重。",
        ],
        "focus_slices": [
            {
                "label": "冬末前季风晴日分风向表现",
                "months": [3, 4, 5],
                "conditions": {
                    "midday_low_ceiling_flag": False,
                    "precip_day_flag": False,
                },
                "sectors": ["E", "SE", "W", "NW"],
                "min_days": 70,
                "min_sector_days": 12,
                "note": "用于区分前季风干混合高温样本与西侧/西北侧偏干风背景，重点看 Tmax 和日较差。",
            }
        ],
    },
}
