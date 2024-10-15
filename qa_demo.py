# encoding=utf-8

"""

@author: SimmerChan

@contact: hsl7698590@gmail.com

@file: query_main.py

@time: 2017/12/20 15:29

@desc:main函数，整合整个处理流程。

"""
import jena_sparql_endpoint
import os
import itertools
from tqdm import tqdm
import math
import heapq
import numpy as np
import jieba

file_path = os.path.split(os.path.realpath(__file__))[0]
prefix = u"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX : <http://www.zhonghuapu.com#>
"""


class QAInterface:
    def __init__(self, myId):
        self.fuseki = jena_sparql_endpoint.JenaFuseki()
        self.myId = myId
        self.max_answer_number = 1
        self.max_path_len = 3
        self.scale = 1
        self.relation_rank = ['父亲', '母亲',
                              '儿子', '女儿',
                              '兄', '弟', '姐', '妹',
                              '丈夫', '妻子',
                              '爷爷', '奶奶',
                              '孙子', '孙女',
                              '伯伯', '叔叔', '侄子', '侄女', '姑姑',
                              '曾祖父', '曾祖母',
                              '曾孙', '曾孙女',
                              '高祖父', '高祖母',
                              '玄孙', '玄孙女',
                              '父母', '子女', '配偶', '兄弟', '姐妹', '兄弟姐妹',
                              '后代', '祖先',
                              '岳父', '岳母',
                              '公公', '婆婆',
                              '小舅子'
                              ]
        self.relation_score = [-10, -9,
                               -9, -8,
                               -7, -7, -7, -7,
                               -10, -7,
                               -6, -5,
                               -6, -6,
                               -4, -4, -4, -4, -4,
                               -5, -4,
                               -5, -5,
                               -4, -3,
                               -4, -4,
                               -3, -3, -2, -2, -2, -1,
                               -3, -3,
                               -8, -7,
                               -8, -8,
                               -6
                               ]
        self.rels_map = {
            '兄': '哥哥',
            '弟': '弟弟',
            '姐': '姐姐',
            '妹': '妹妹'
        }

    def answer(self, name: str, rel: str, father_name1: str,father_name2="", attributes=""):
        if rel == "叔伯":
            my_query = self.get_age_query(name,"爷爷","儿子",father_name1,father_name2)
        elif rel == "兄弟1":
            my_query = self.get_age_query(name,"父亲","儿子",father_name1,father_name2)
        elif rel == "姐妹1":
            my_query = self.get_age_query(name,"父亲", "女儿",father_name1,father_name2)
        elif rel == "兄弟姐妹1":
            my_query = self.get_age_query(name, "父亲", "子女", father_name1,father_name2)
        elif rel != "":
            my_query = self.get_query(name,rel,father_name1,father_name2)
        else:
            my_query = self.find_intro(name,father_name1,attributes)
        ans = {
            "names": [],
            "gens": [],
            "sexs": [],
            "ranks": [],
            "ages": [],
            "intros": [],
            "is_poss":[],
            "atts":[],
        }
        # print(my_query,rel)
        if my_query is not None:
            result = self.fuseki.get_sparql_result(my_query)

            if "results" in result and "bindings" in result['results']:
                value = result['results']['bindings']
                for res in value:
                    if (res["names"]['value'] in ans["names"]) and (res["intros"]['value'] in ans["intros"]):#当前人物已经存在结果中
                        continue
                    for var in ans:# 遍历需要的属性
                        if var in res:# 判断查询结果中存不存在
                            ans[var].append(res[var]["value"])
                            if var == "ages":
                                ans[var][-1] = ans[var][-1].split("年")[0]
                                ans[var][-1] = ans[var][-1].split(".")[0]
                        else:# 不存在则为空值
                            ans[var].append("")
                    ans["is_poss"][-1] = 0# 先将所有人物都认为是不是可能的（需要根据年龄判断的关系）
                if rel in ["叔叔","伯伯","弟","妹","兄","姐","兄弟",'姐妹',"兄弟姐妹"]:
                    father = self.answer(name, "父亲", father_name1)
                    if father["ages"]:
                        father["ages"][0] = father["ages"][0].split("年")[0]
                        father["ages"][0] = father["ages"][0].split(".")[0]
                    person = self.answer(name, "", father_name1)
                    if person["ages"]:
                        person["ages"][0] = person["ages"][0].split("年")[0]
                        person["ages"][0] = person["ages"][0].split(".")[0]
                    ans_poss_sb_has_father = self.answer(name,"叔伯",father_name1,father_name2)
                    ans_poss_sb = {}
                    if father["names"]:
                        for key in ans:
                            ans_poss_sb[key] = [n for ids,n in enumerate(ans_poss_sb_has_father[key]) if ans_poss_sb_has_father["names"][ids] not in father["names"]]
                    else:
                        ans_poss_sb = ans_poss_sb_has_father
                    ans_poss_xd = self.answer(name, "兄弟1", father_name1,father_name2)
                    ans_poss_jm = self.answer(name, "姐妹1", father_name1,father_name2)
                    ans_poss_xdjm = self.answer(name,"兄弟姐妹1",father_name1,father_name2)
                    if rel == "兄弟":
                        ans = self.merge_ans_ans_poss(ans,ans_poss_xd)
                    if rel == "姐妹":
                        # print("姐妹1:", ans_poss_jm)
                        ans = self.merge_ans_ans_poss(ans,ans_poss_jm)
                        # print("姐妹_ans:", ans)
                    if rel == "兄弟姐妹":
                        # print("兄弟姐妹1:",ans_poss_xdjm)
                        ans = self.merge_ans_ans_poss(ans,ans_poss_xdjm)
                    if rel == "叔叔":
                        ans_poss = self.judge_younger(ans_poss_sb, father)
                        ans = self.merge_ans_ans_poss(ans,ans_poss)
                    if rel == "伯伯":
                        ans_poss = self.judge_elder(ans_poss_sb, father)
                        ans = self.merge_ans_ans_poss(ans, ans_poss)
                    if rel == "弟" or rel == "妹":
                        if rel == "弟":
                            ans_poss = self.judge_younger(ans_poss_xd, person)
                        else:
                            # print("妹妹_poss:", ans_poss_jm)
                            ans_poss = self.judge_younger(ans_poss_jm, person)
                            # print("妹妹_ans:", ans_poss)
                        ans = self.merge_ans_ans_poss(ans, ans_poss)
                    if rel == "兄" or rel == "姐":
                        if rel == "兄":
                            ans_poss = self.judge_elder(ans_poss_xd, person)
                        else:
                            ans_poss = self.judge_elder(ans_poss_jm, person)
                        ans = self.merge_ans_ans_poss(ans, ans_poss)
        return ans

    def merge_ans_ans_poss(self,ans,ans_poss):
        for i in range(0,len(ans_poss["names"])):
            if ans_poss["names"][i] in ans["names"] and ans_poss["intros"][i] in ans["intros"]:
                continue
            for key in ans_poss:
                ans[key].append(ans_poss[key][i])
        return ans

    def judge_younger(self,ans,person):
        new_ans = {
            "names": [],
            "gens": [],
            "sexs": [],
            "ranks": [],
            "ages": [],
            "intros": [],
            "is_poss":[]
        }
        # person = self.answer(name,"")#获取自己的信息
        # print(person, ans)
        for ids in range(len(ans["names"])):
            if ans["sexs"][ids] == person["sexs"][0]:#如果性别相同，则使用排行判断
                if int(ans["ranks"][ids]) > int(person["ranks"][0]):
                    for var in new_ans:
                        new_ans[var].append(ans[var][ids])
                    new_ans["is_poss"][-1] = 0
            else:# 性别不同，使用出生日期判断
                if ans["ages"][ids].isdigit() and person["ages"][0].isdigit():# 出生日期均为数字才能判断
                    if int(ans["ages"][ids]) > int(person["ages"][0]):
                        for var in new_ans:
                            new_ans[var].append(ans[var][ids])
                        new_ans["is_poss"][-1] = 0
                else:  # Todo则为可能的结果
                    for var in new_ans:
                        new_ans[var].append(ans[var][ids])
                    new_ans["is_poss"][-1] = 1

        return new_ans
    def judge_elder(self,ans,person):
        new_ans = {
            "names": [],
            "gens": [],
            "sexs": [],
            "ranks": [],
            "ages": [],
            "intros": [],
            "is_poss": [],
        }
        # person = self.answer(name, "")  # 获取自己的信息
        # print(person,ans)
        for ids in range(len(ans["names"])):
            if ans["sexs"][ids] == person["sexs"][0]:  # 如果性别相同，则使用排行判断
                if int(ans["ranks"][ids]) < int(person["ranks"][0]):
                    for var in new_ans:
                        new_ans[var].append(ans[var][ids])
                    new_ans["is_poss"][-1] = 0
            else:  # 性别不同，使用出生日期判断
                if ans["ages"][ids].isdigit() and person["ages"][0].isdigit():  # 出生日期均为数字才能判断
                    if int(ans["ages"][ids]) < int(person["ages"][0]):
                        for var in new_ans:
                            new_ans[var].append(ans[var][ids])
                        new_ans["is_poss"][-1] = 0
                else:#Todo则为可能的结果
                    for var in new_ans:
                        new_ans[var].append(ans[var][ids])
                    new_ans["is_poss"][-1] = 1

        return new_ans

    def get_age_query(self,name,rel1,rel2,father_name1,father_name2):
        expression = f"?a :姓名 '{name}'.\n" \
                     f"?a :{rel1} ?b.\n" \
                     f"?b :{rel2} ?f.\n" \
                     f"?f :姓名 ?names."
        if father_name1 != "":
            expression += f"\n?a :父亲 ?father1."
            expression += f"\n?father1 :姓名 '{father_name1}'."
        if father_name2 != "":
            expression += f"\n?f :父亲 ?father2."
            expression += f"\n?father2 :姓名 '{father_name2}'."

        if self.myId != "0":
            s = '", "'.join(self.myId)
            expression += "\n FILTER(?myId IN (\"" + s + "\"))"
        expression += "\n FILTER(?names != \"" + name + "\")"

        query_body = u"{prefix}\n" + \
                     u"SELECT DISTINCT ?names (STR(?fg) AS ?gens) ?sexs (STR(?fr) AS ?ranks) ?ages ?intros WHERE {{\n" + \
                     u"{expression}\n" + \
                     u"}}\n"
        expression += "\nOPTIONAL { ?f :世代 ?fg;} OPTIONAL { ?f :性别 ?sexs;} OPTIONAL { ?f  :排行 ?fr;} OPTIONAL { ?f  :出生日期 ?ages;} OPTIONAL { ?f  :简介 ?intros} OPTIONAL { ?f  :家谱 ?myId}"
        query = query_body.format(prefix=prefix, expression=expression)
        return query

    def get_query(self,name,rel,father_name1,father_name2):
        expression = f"?a :姓名 '{name}'.\n" \
                     f"?a :{rel} ?f.\n" \
                     f"?f :姓名 ?names."
        if father_name1 != "":
            expression += f"\n?a :父亲 ?father1."
            expression += f"\n?father1 :姓名 '{father_name1}'."
        if father_name2 != "":
            expression += f"\n?f :父亲 ?father2."
            expression += f"\n?father2 :姓名 '{father_name2}'."

        if self.myId != "0":
            s = '", "'.join(self.myId)
            expression += "\n FILTER(?myId IN (\"" + s + "\"))"
        expression += "\n FILTER(?names != \"" + name + "\")"

        query_body = u"{prefix}\n" + \
                     u"SELECT DISTINCT ?names (STR(?fg) AS ?gens) ?sexs (STR(?fr) AS ?ranks) ?ages ?intros WHERE {{\n" + \
                     u"{expression}\n" + \
                     u"}}\n"
        expression += "\nOPTIONAL { ?f :世代 ?fg;} OPTIONAL { ?f :性别 ?sexs;} OPTIONAL { ?f  :排行 ?fr;} OPTIONAL { ?f  :出生日期 ?ages;} OPTIONAL { ?f  :简介 ?intros} OPTIONAL { ?f  :家谱 ?myId}"
        if rel == "祖先":
            query_body+="ORDER BY DESC(?fg)"
        elif rel == "后代":
            query_body += "ORDER BY ?fg"
        query = query_body.format(prefix=prefix, expression=expression)
        return query

    def find_intro(self, anchor_name,father_name,attributes):
        expression = u"?f :简介 ?intros; :姓名 ?names.\n" \
                     u"?f :姓名 '{anchor_name}'.".format(anchor_name=anchor_name)

        if father_name != "":
            expression += f"\n?f :父亲 ?father."
            expression += f"\n?father :姓名 '{father_name}'."

        if self.myId != "0":
            s = '", "'.join(self.myId)
            expression += "\n FILTER(?myId IN (\"" + s + "\"))"

        query_body = u"{prefix}\n" + \
                     u"SELECT DISTINCT ?names (STR(?fg) AS ?gens) ?sexs (STR(?fr) AS ?ranks) ?ages ?intros ?atts WHERE {{\n" + \
                     u"{expression}\n" + \
                     u"}}\n"
        expression += ("\nOPTIONAL { ?f :世代 ?fg;} OPTIONAL { ?f :性别 ?sexs;} OPTIONAL { ?f  :排行 ?fr;} OPTIONAL { ?f  "
                       ":出生日期 ?ages;} OPTIONAL { ?f  :简介 ?intros} OPTIONAL { ?f  :家谱 ?myId} OPTIONAL { ?f "
                       ":")+attributes+" ?atts}"
        query = query_body.format(prefix=prefix, expression=expression)
        return query

    def find_user_path_user(self, name1, name2,father_name1,father_name2):
        path = ""  # 查询的路径
        names = ""  # 查询中间人物的名字h'h'h
        res_name = ""  # 需要返回的关系和人名
        path_list = []  # 保存所有可能的路径
        sign = False
        # relation_rank = self.relation_rank
        # relation_score = self.reltion_score
        for i in range(1, 5):

            # 获取路径组合
            if i == 1:
                relation_rank = self.relation_rank
                relation_score = self.relation_score
            else:
                relation_cache = []
                score_cache = []
                for _ in range(i):
                    relation_cache.append(self.relation_rank)
                    score_cache.append(self.relation_score)
                relation_rank = list(itertools.product(*relation_cache))
                relation_score = [self.relation_score_path_decay(relation_tuple) for relation_tuple in
                                  list(itertools.product(*score_cache))]
            sorted_indices = np.argsort(relation_score)

            head = f"?p0 :姓名 '{name1}'; :家谱 ?myId1; :简介 ?intro1.\n"
            if father_name1 != "":
                head += f"?p0 :父亲 ?father1.\n?father1 :姓名 ?father1_name ."
            path += f"\n?p{str(i - 1)} ?rel{str(i)} ?p{str(i)}."
            res_name += f" ?rel{str(i)}"
            tail = f"\n?p{str(i)} :姓名 \"{name2}\"; :家谱 ?myId2; :简介 ?intro2.\n"
            if father_name2 != "":
                tail += f"?p{str(i)} :父亲 ?father2.\n?father2 :姓名 ?father2_name ."
            filter_myId = f"\nFILTER(?myId1 = \"{self.myId}\")\nFILTER(?myId2 = \"{self.myId}\")\n"
            filter_father_name = ""
            if father_name1 != "":
                filter_father_name += f"\nFILTER(?father1_name = \"{father_name1}\")\n"
            if father_name2 != "":
                filter_father_name += f"FILTER(?father2_name = \"{father_name2}\")\n"
            print(f'搜索{i}跳关系')

            # 路径遍历
            for relation_tuple_idx in tqdm(sorted_indices):
                relation_tuple = relation_rank[relation_tuple_idx]
                if i == 1:
                    filter_rel = f"FILTER(?rel{str(i)} = :{relation_tuple})"
                else:
                    filter_rel = '\n'.join(
                        [f"FILTER(?rel{str(j + 1)} = :{relation_tuple[j]})" for j in range(len(relation_tuple))])
                query_body = f"{prefix}\n" + \
                             f"SELECT {res_name} ?intro1 ?intro2 WHERE " + "{{\n" + \
                             u"{expression}\n" + \
                             u"}} LIMIT 25\n"
                query = query_body.format(expression=head + path + names + tail + filter_myId + filter_rel + filter_father_name)
                # print("query:", query)
                result = self.fuseki.get_sparql_result(query)  # 执行查询语句
                if "results" in result and "bindings" in result['results'] and result['results']['bindings']:  # 取出结果
                    # print(result)
                    value = result['results']['bindings']
                    for v1 in value:
                        paths = []
                        paths.append(name1)
                        for v2 in v1:
                            paths.append((v1[v2]["value"].split("http://www.zhonghuapu.com#"))[-1])
                            # print((v1[v2]["value"].split("http://www.zhonghuapu.com#"))[-1], end=" ")
                        paths.insert(-2,name2)
                        if (len(paths) // 2) < self.max_path_len and (len(path_list) < self.max_answer_number):
                            path_list.append(paths)
                        else:
                            # # TODO 把print的结果反馈到界面。
                            # print('抱歉，在最大路径长度内未找到合适的答案。调节最大路径长度上限可以让系统做更深入地查询')
                            continue
                if (len(path_list) == self.max_answer_number) or (i == self.max_path_len):
                    sign = True

            names += "\n?p" + str(i) + " :姓名 ?n" + str(i) + "."
            res_name += " ?n" + str(i)

            if sign:
                break

        res = ""
        intro1 = ""
        intro2 = ""
        for p in path_list:
            for i in range(0, len(p) - 4, 2):
                if p[i + 1] in self.rels_map:
                    r = self.rels_map[p[i + 1]]
                else:
                    r = p[i + 1]
                res += f"{p[i]}的{r}是{p[i + 2]}。"
            intro1 = p[len(p) - 2]
            intro2 = p[len(p) - 1]
            res += "\n"

        return sign, res, self.max_answer_number, self.max_path_len,intro1,intro2

    def relation_score_path_decay(self, relation_score_path, merge_relation=None):
        # TODO 路径衰减率可调。
        if merge_relation != None:  # find_user_path_user_heap_sorting里
            path_len = len(merge_relation[0].split(';'))
        else:
            path_len = len(relation_score_path)
        score = relation_score_path[0] * (path_len - 1)
        if path_len > 1:
            score += relation_score_path[1] * math.exp((1 - path_len) * self.scale)
        return score / path_len

    def merge_multi_relation(self, relation_list):
        if len(relation_list) == 1:
            return relation_list
        else:
            return [';'.join(relation_list)]

    def find_user_path_user_heap_sorting(self, name1, name2, father_name1, father_name2):

        path_list = []  # 保存所有可能的路径
        gen_list = []
        has_ancenstor_list = []
        sign = False
        relation_score = []
        top_relation_path_score = []
        top_relation_path = []
        i = 0
        # for i in range(1, 5):
        while not sign:
            # 获取路径组合
            if i == 0:
                relation_score = [
                    (
                        [self.relation_score[k]],
                        [self.relation_rank[k]]
                    ) for k in range(len(self.relation_score))
                ]
                heapq.heapify(relation_score)  # 建堆
            if i < len(self.relation_rank) and len(relation_score) > 0:
                # i += 1
                assert len(relation_score) > 0
            else:
                if i == len(self.relation_rank):
                    relation_rank = list(itertools.product(self.relation_rank[:-2], self.relation_rank[:-2]))
                    relation_score_tuple = list(itertools.product(self.relation_score[:-2], self.relation_score[:-2]))
                    relation_rank.append(('丈夫', '祖先'))
                    relation_score_tuple.append(
                        (self.relation_score[self.relation_rank.index('丈夫')],
                         self.relation_score[self.relation_rank.index('祖先')])
                    )
                    relation_rank.append(('丈夫', '后代'))
                    relation_score_tuple.append(
                        (self.relation_score[self.relation_rank.index('丈夫')],
                         self.relation_score[self.relation_rank.index('后代')])
                    )
                    relation_rank.append(('妻子', '祖先'))
                    relation_score_tuple.append(
                        (self.relation_score[self.relation_rank.index('妻子')],
                         self.relation_score[self.relation_rank.index('祖先')])
                    )
                    relation_rank.append(('妻子', '后代'))
                    relation_score_tuple.append(
                        (self.relation_score[self.relation_rank.index('妻子')],
                         self.relation_score[self.relation_rank.index('后代')])
                    )
                    relation_rank.append(('祖先', '后代'))
                    relation_score_tuple.append(
                        (self.relation_score[self.relation_rank.index('祖先')],
                         self.relation_score[self.relation_rank.index('后代')])
                    )
                    relation_rank.append(('后代', '丈夫'))
                    relation_score_tuple.append(
                        (self.relation_score[self.relation_rank.index('后代')],
                         self.relation_score[self.relation_rank.index('丈夫')])
                    )
                    relation_rank.append(('后代', '妻子'))
                    relation_score_tuple.append(
                        (self.relation_score[self.relation_rank.index('后代')],
                         self.relation_score[self.relation_rank.index('妻子')])
                    )
                    for k in range(len(relation_score_tuple)):
                        merge_relation = self.merge_multi_relation(relation_rank[k])
                        heapq.heappush(relation_score,
                                       ([self.relation_score_path_decay(relation_score_tuple[k], merge_relation)],
                                        merge_relation
                                        )
                                       )
                if len(top_relation_path[0].split(';')) < self.max_path_len:
                    if top_relation_path[0] in ['丈夫;祖先', '妻子;祖先']:
                        candidate_rel = self.relation_rank[:-1]
                        candidate_rel_score = self.relation_score[:-1]
                    elif top_relation_path[0] == '祖先;后代':
                        candidate_rel = ['丈夫', '妻子']
                        candidate_rel_score = [self.relation_score[self.relation_rank.index('丈夫')],
                                               self.relation_score[self.relation_rank.index('妻子')]]
                    else:
                        candidate_rel = self.relation_rank[:-2]
                        candidate_rel_score = self.relation_score[:-2]

                    relation_rank = list(itertools.product(top_relation_path, candidate_rel))
                    relation_score_tuple = list(itertools.product(top_relation_path_score, candidate_rel_score))
                    for k in range(len(relation_score_tuple)):
                        merge_relation = self.merge_multi_relation(relation_rank[k])
                        heapq.heappush(relation_score,
                                       ([self.relation_score_path_decay(relation_score_tuple[k], merge_relation)],
                                        merge_relation
                                        )
                                       )
                i += 1

            i += 1
            top_relation_path_score, top_relation_path = heapq.heappop(relation_score)
            # if len(top_relation_path[0].split(';')) == 3:
            #     print()
            # print(i, top_relation_path_score, top_relation_path)
            head = f"?p0 :姓名 '{name1}'; :家谱 ?myId1; :简介 ?intro1; :世代 ?gen0.\n"
            if father_name1 != "":
                head += '?p0 :父亲 ?father1.\n?father1 :姓名 ?father1_name.'
            if ';' in top_relation_path[0]:#多跳关系
                top_relation_path_list = top_relation_path[0].split(';')
                path = ''.join(
                    [f"\n?p{str(j)} ?rel{str(j + 1)} ?p{str(j + 1)}." for j in range(len(top_relation_path_list))])
                res_name = '?gen0 ?rel1' + ''.join(
                    [f" ?n{str(j + 1)} ?gen{str(j + 1)} ?rel{str(j + 2)}" for j in range(len(top_relation_path_list) - 1)])
                res_name += f" ?gen{str(len(top_relation_path_list))}"
                # gen = ''.join([f" ?gen{str(i)}" for i in range(len(top_relation_path_list) + 1)])
                names = ''.join([f"\n?p{str(j + 1)} :姓名 ?n{str(j + 1)}; :世代 ?gen{str(j + 1)}." for j in range(len(top_relation_path_list) - 1)])
                tail = f"\n?p{str(len(top_relation_path_list))} :姓名 \"{name2}\"; :家谱 ?myId2; :简介 ?intro2; :世代 ?gen{str(len(top_relation_path_list))}.\n"
                if father_name2 != "":
                    tail += f'?p{str(len(top_relation_path_list))} :父亲 ?father2.\n?father2 :姓名 ?father2_name.'
                filter_rel = '\n'.join(
                    [f"FILTER(?rel{str(j + 1)} = :{top_relation_path_list[j]})" for j in
                     range(len(top_relation_path_list))])
            else:#一跳关系
                path = "\n?p0 ?rel1 ?p1."
                res_name = "?gen0 ?rel1 ?gen1"
                # gen = " ?gen0 ?gen1"
                names = ''
                tail = f"\n?p1 :姓名 \"{name2}\"; :家谱 ?myId2; :简介 ?intro2; :世代 ?gen1.\n"
                if father_name2 != "":
                    tail += f'?p1 :父亲 ?father2.\n?father2 :姓名 ?father2_name.'
                filter_rel = f"FILTER(?rel1 = :{top_relation_path[0]})"
            s = '", "'.join(self.myId)
            filter_myId = f"\nFILTER(?myId1 IN (\"" + s + "\"))\nFILTER(?myId2 IN (\"" + s + "\"))\n"
            filter_father_name = ""
            if father_name1 != "":
                filter_father_name += f"\nFILTER(?father1_name = \"{father_name1}\")\n"
            if father_name2 != "":
                filter_father_name += f"FILTER(?father2_name = \"{father_name2}\")\n"
            query_body = f"{prefix}\n" + \
                         f"SELECT {res_name} ?intro1 ?intro2 WHERE " + "{{\n" + \
                         u"{expression}\n" + \
                         u"}} LIMIT 1\n"
            query = query_body.format(expression=head + path + names + tail + filter_myId + filter_rel + filter_father_name)
            # print("query:", query)
            result = self.fuseki.get_sparql_result(query)  # 执行查询语句

            if "results" in result and "bindings" in result['results'] and result['results']['bindings']:  # 取出结果
                # print(result)
                value = result['results']['bindings']
                for v1 in value:
                    paths = []
                    paths.append(name1)
                    gens = []
                    has_ancenstor = False
                    for v2 in v1:
                        if 'gen' in v2:
                            gens.append(v1[v2]["value"])
                        else:
                            if (v1[v2]["value"].split("http://www.zhonghuapu.com#"))[-1] in ['祖先', '后代']:
                                has_ancenstor = True
                            paths.append((v1[v2]["value"].split("http://www.zhonghuapu.com#"))[-1])
                        # print((v1[v2]["value"].split("http://www.zhonghuapu.com#"))[-1], end=" ")
                    paths.insert(-2, name2)
                    # if (len(paths) // 2) < self.max_path_len and (len(path_list) < self.max_answer_number):
                    if len(path_list) < self.max_answer_number:
                        path_list.append(paths)
                        gen_list.append(gens)
                        has_ancenstor_list.append(has_ancenstor)
                        # print(path_list)
                    else:
                        # # TODO 把print的结果反馈到界面。
                        # print('抱歉，在最大路径长度内未找到合适的答案。调节最大路径长度上限可以让系统做更深入地查询')
                        continue

            if (len(path_list) == self.max_answer_number) or (len(relation_score) == 0 and i > len(self.relation_rank)):
                sign = True

        res = ""
        intro1 = ""
        intro2 = ""
        for k in range(len(path_list)):
            p = path_list[k]
            gens = gen_list[k]
            # has_ancenstor = has_ancenstor_list[k]
            for _ in range(0, len(p) - 4, 2):
                if p[_ + 1] in self.rels_map:
                    r = self.rels_map[p[_ + 1]]
                else:
                    r = p[_ + 1]
                # if has_ancenstor:
                #     res += f"{p[_]}（{gens[_//2]}世）的{r}是{p[_ + 2]}（{gens[(_//2)+1]}世）。"
                # else:
                #     res += f"{p[_]}的{r}是{p[_ + 2]}。"
                if len(p) == 5:
                    res += f"{p[_ + 2]}是{p[_]}的{r}。"
                elif len(p) == 7:
                    if p[1] in self.rels_map:
                        r1 = self.rels_map[p[1]]
                    else:
                        r1 = p[1]
                    if p[3] in self.rels_map:
                        r2 = self.rels_map[p[3]]
                    else:
                        r2 = p[3]
                    name_list = [p[0], p[2], p[4]]
                    rel_list = [r1, r2]
                    name_list, rel_list = self.merge_rel(name_list, rel_list)
                    if len(rel_list) == 1:
                        res += f"{name_list[0]}是{name_list[-1]}的{rel_list[0]}。"
                    else:
                        res += f"{p[4]}是{p[0]}{r1}的{r2}。"
                    break
                elif len(p) == 9:
                    if p[1] in self.rels_map:
                        r1 = self.rels_map[p[1]]
                    else:
                        r1 = p[1]
                    if p[3] in self.rels_map:
                        r2 = self.rels_map[p[3]]
                    else:
                        r2 = p[3]
                    if p[5] in self.rels_map:
                        r3 = self.rels_map[p[5]]
                    else:
                        r3 = p[5]
                    name_list = [p[0], p[2], p[4], p[6]]
                    rel_list = [r1, r2, r3]
                    name_list, rel_list = self.merge_rel(name_list, rel_list)
                    if len(rel_list) == 2:
                        res += f"{name_list[0]}是{name_list[-1]}{rel_list[0]}的{rel_list[1]}。"
                        break
                    else:
                        res += f"{p[_]}的{r}是{p[_ + 2]}。"
                else:
                    res += f"{p[_]}的{r}是{p[_ + 2]}。"
            intro1 = p[len(p)-2]
            intro2 = p[len(p)-1]
            res += "\n"

        return sign, res, self.max_answer_number, self.max_path_len,intro1,intro2
    def merge_rel(self, name_list, rel_list):
        merge_rel_dict = {'丈夫;爷爷': '孙媳妇',
                          '丈夫;奶奶': '孙媳妇',
                          '女儿;丈夫': '岳父',
                          '儿子;岳父': '亲家'
                          }
        if ';'.join(rel_list) == '丈夫;父亲;伯伯':
            name_list = [name_list[0], name_list[-1]]
            rel_list = ['侄子', '儿媳妇']
        elif ';'.join(rel_list) in merge_rel_dict:
            name_list = [name_list[0], name_list[-1]]
            rel_list = [merge_rel_dict[';'.join(rel_list)]]

        return name_list, rel_list

    def find_userId(self,name,father_name):
        expression = f"?f :姓名 '{name}'.\n"

        if father_name != "":
            expression += f"\n?f :父亲 ?father."
            expression += f"\n?father :姓名 '{father_name}'."

        if self.myId != "0":
            s = '", "'.join(self.myId)
            expression += "\n FILTER(?myId IN (\"" + s + "\"))"

        query_body = u"{prefix}\n" + \
                     u"SELECT DISTINCT ?f WHERE {{\n" + \
                     u"{expression}\n" + \
                     u"}}\n"
        expression += ("\n OPTIONAL { ?f  :家谱 ?myId}  ")
        my_query = query_body.format(prefix=prefix, expression=expression)
        # print("my_query:", my_query)
        result = self.fuseki.get_sparql_result(my_query)
        # print("result",result)
        userIds = []
        if "results" in result and "bindings" in result['results']:
            value = result['results']['bindings']
            for v in value:
                userIds.append(v['f']['value'].split('#person/')[-1])
        return userIds

def match_same_people_intro(names, question, qa_interface, myId):
    rels = {
        "兄弟姐妹": "兄弟姐妹",
        "兄弟": "兄弟",
        "姐妹": "姐妹",
        '父亲': '父亲',
        # '父': '父亲',
        '爹': '父亲',
        '爸爸': '父亲',
        '爸': '父亲',
        '母亲': '母亲',
        # '母': '母亲',
        '娘': '母亲',
        '妈妈': '母亲',
        '妈': '母亲',
        '儿子': '儿子',
        '之子': '儿子',
        # '子': '儿子',
        '女儿': '女儿',
        '闺女': '女儿',
        '之女': '女儿',
        # '女': '女儿',
        '哥哥': '兄',
        '哥': '兄',
        '兄': '兄',
        '弟弟': '弟',
        '弟': '弟',
        '姊妹': '姐妹',
        '姐姐': '姐',
        '姐': '姐',
        '姊': '姐',
        '妹妹': '妹',
        '妹': '妹',
        '丈夫': '丈夫',
        '夫': '丈夫',
        '老公': '丈夫',
        '妻子': '妻子',
        '妻': '妻子',
        '老婆': '妻子',
        '嗣父': '继父',
        '继父': '继父',
        '养父': '继父',
        '过继的嗣子': '继子',
        '继子': '继子',
        '养子': '继子',
        "父母": "父母",
        "子女": "子女",
        "曾祖父": "曾祖父",
        "曾祖母": "曾祖母",
        "曾祖": "曾祖",
        "曾孙女": "曾孙女",
        "曾孙": "曾孙",
        "高祖父": "高祖父",
        "高祖母": "高祖母",
        "高祖": "高祖",
        "玄孙女": "玄孙女",
        "玄孙": "玄孙",
        "祖先": "祖先",
        "后代": "后代",
        "伯伯": "伯伯",
        "叔叔": "叔叔",
        "姑姑": "姑姑",
        "侄子": "侄子",
        "侄女": "侄女",
        "奶奶": "奶奶",
        "爷爷": "爷爷",
        "孙子": "孙子",
        "孙女": "孙女",
        "岳父": "岳父",
        '岳丈': "岳父",
        "岳母": "岳母",
        "公公": "公公",
        "婆婆": "婆婆",
        "小舅子": "小舅子"
    }

    multi_people_infor_dict = {}
    is_same_people = False
    for i in range(len(names)):
        people_infor_dict = {}
        name = names[i]

        prefix = u'''prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix owl: <http://www.w3.org/2002/07/owl#>
        prefix xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX : <http://www.zhonghuapu.com#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX math: <http://jena.hpl.hp.com/ARQ/math#>
        SELECT DISTINCT ?intro ?father ?mother ?son ?daughter ?orderbrother ?youngerbrother ?ordersister ?youngersister ?husband ?wife ?olderuncle ?youngeruncle ?aunt ?nephew ?niece ?grandpa ?grandma ?grandson ?granddaughter WHERE {
        '''
        anchor_query = f'?p0 :姓名 "{name}"; :简介 ?intro.\n'
        tail_query = '''OPTIONAL{?p0 :父亲 ?p1. ?p1 :姓名 ?father}.
        OPTIONAL{?p0 :母亲 ?p2. ?p2 :姓名 ?mother}.
        OPTIONAL{?p0 :儿子 ?p3. ?p3 :姓名 ?son}.
        OPTIONAL{?p0 :女儿 ?p4. ?p4 :姓名 ?daughter}.
        OPTIONAL{?p0 :兄 ?p5. ?p5 :姓名 ?orderbrother}.
        OPTIONAL{?p0 :弟 ?p6. ?p6 :姓名 ?youngerbrother}.
        OPTIONAL{?p0 :姐 ?p7. ?p7 :姓名 ?ordersister}.
        OPTIONAL{?p0 :妹 ?p8. ?p8 :姓名 ?youngersister}.
        OPTIONAL{?p0 :丈夫 ?p9. ?p9 :姓名 ?husband}.
        OPTIONAL{?p0 :妻子 ?p10. ?p10 :姓名 ?wife}.
        OPTIONAL{?p0 :伯伯 ?p11. ?p11 :姓名 ?olderuncle}.
        OPTIONAL{?p0 :叔叔 ?p12. ?p12 :姓名 ?youngeruncle}.
        OPTIONAL{?p0 :姑姑 ?p13. ?p13 :姓名 ?aunt}.
        OPTIONAL{?p0 :侄子 ?p14. ?p14 :姓名 ?nephew}.
        OPTIONAL{?p0 :侄女 ?p15. ?p15 :姓名 ?niece}.
        OPTIONAL{?p0 :爷爷 ?p16. ?p16 :姓名 ?grandpa}.
        OPTIONAL{?p0 :奶奶 ?p17. ?p17 :姓名 ?grandma}.
        OPTIONAL{?p0 :孙子 ?p18. ?p18 :姓名 ?grandson}.
        OPTIONAL{?p0 :孙女 ?p19. ?p19 :姓名 ?granddaughter}.
        OPTIONAL{?p0 :家谱 ?myId}
        '''
        if myId != "0":
            s = '", "'.join(myId)
            tail_query += "\n FILTER(?myId IN (\"" + s + "\"))}"
        query = prefix + anchor_query + tail_query
        ans = qa_interface.fuseki.get_sparql_result(query)
        # keys = ans['head']['vars']
        results = ans['results']['bindings']
        flag_father = ''
        people_infor = {}
        ## 获取同名人物的每个人的信息
        for res in results:
            if 'father' not in res:
                continue
            if flag_father == '':
                flag_father = res['father']['value']
            if flag_father != res['father']['value']:
                if flag_father not in people_infor_dict:
                    people_infor_dict[flag_father] = people_infor
                else:
                    union_keys = list(set(list(people_infor.keys()) + list(people_infor_dict[flag_father].keys())))
                    for union_key in union_keys:
                        if union_key in people_infor and union_key in people_infor_dict[flag_father]:
                            people_infor_dict[flag_father][union_key].extend(people_infor[union_key])
                            people_infor_dict[flag_father][union_key] = list(
                                set(people_infor_dict[flag_father][union_key]))
                        elif union_key in people_infor and union_key not in people_infor_dict[flag_father]:
                            people_infor_dict[flag_father][union_key] = people_infor[union_key]
                flag_father = res['father']['value']
                people_infor = {}
            for k in res.keys():
                if k not in people_infor:
                    people_infor[k] = [res[k]['value']]
                else:
                    if res[k]['value'] not in people_infor[k]:
                        people_infor[k].append(res[k]['value'])

        if flag_father not in people_infor_dict:
            people_infor_dict[flag_father] = people_infor
        else:
            union_keys = list(set(list(people_infor.keys()) + list(people_infor_dict[flag_father].keys())))
            for union_key in union_keys:
                if union_key in people_infor and union_key in people_infor_dict[flag_father]:
                    people_infor_dict[flag_father][union_key].extend(people_infor[union_key])
                    people_infor_dict[flag_father][union_key] = list(set(people_infor_dict[flag_father][union_key]))
                elif union_key in people_infor and union_key not in people_infor_dict[flag_father]:
                    people_infor_dict[flag_father][union_key] = people_infor[union_key]

        keys = people_infor_dict.keys()
        for key in keys:
            string = ''
            for infor_key in people_infor_dict[key]:
                info = '@'.join(people_infor_dict[key][infor_key]) + '@'
                string += info
            people_infor_dict[key] = string

        multi_people_infor_dict[name] = people_infor_dict

        if len(people_infor_dict) > 1:
            is_same_people = True

    father_names = [""] * len(names)
    stop_words = ['的', '了', '是', '在', '有', '和', '就', '这', '个', '，', '。', '？', '！', '：', '；', '', ' '] + list(
        rels.keys()) + list(rels.values())
    question_jieba = jieba.lcut(question.strip())
    question = question.replace('的', ' ').replace('是', ' ').replace('？', ' ').replace('。', ' ').replace('，',
                                                                                                        ' ').replace(
        '；', ' ').replace('：', ' ').strip().split(' ')
    question_jieba.extend(question)
    question_jieba = [word for word in question_jieba if word not in stop_words]
    for i in range(len(names)):
        name = names[i]
        people_infor_dict = multi_people_infor_dict[name]
        father_scores = []
        for father in people_infor_dict:
            father_infor = people_infor_dict[father].strip()[:-1].split('@')
            father_intro = father_infor[0]
            father_infor_jieba = jieba.lcut(father_intro)
            father_intro = father_intro.replace('的', ' ').replace('是', ' ').replace('？', ' ').replace('。',
                                                                                                      ' ').replace(
                '，', ' ').replace(
                '；', ' ').replace('：', ' ').strip().split(' ')
            father_infor_jieba.extend(father_intro)
            father_infor_jieba = [word for word in father_infor_jieba if word not in stop_words]
            father_infor_jieba += father_infor[1:]

            intersection = set(question_jieba).intersection(set(father_infor_jieba)).difference(set(names))
            if len(set(question_jieba)) > 0:
                father_scores.append((father, len(intersection) / len(set(question_jieba))))
            else:
                father_scores.append((father, 0.0))
        max_father, max_score = max(father_scores, key=lambda x: x[1])
        min_father, min_score = min(father_scores, key=lambda x: x[1])
        if max_score > min_score:
            father_names[i] = max_father
            is_same_people = False
    return father_names[0], is_same_people

if __name__ == '__main__':
    myId = "3030993" ## 曾国藩家谱id (see '<file:///D:/d2rq-0.8.1/mabuwu.nt#person/14913558> <http://www.zhonghuapu.com#家谱> "3030993" .' in zengguofanjiapu.nt)
    qa_interface = QAInterface(myId)

    ## 多条件匹配机制（筛选同名人物）
    ## 我们默认用一个人的父亲来标识同名人物的具体个体
    father_names, is_same_people = match_same_people_intro(names=['陈氏'], question='陈氏出生于长沙', qa_interface=qa_interface, myId=myId)
    if is_same_people:
        father_names = ''
    ## 路径排序
    res = qa_interface.find_user_path_user_heap_sorting(name1='陈氏', name2='曾广銮', father_name1=father_names, father_name2='')
