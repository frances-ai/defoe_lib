import os
import subprocess
import re
import textwrap
import time

from lxml import etree

# change this according to your path
defoe_path = "/Users/ly40/Documents/frances-ai/defoe_lib/geoparser/"
# os_type = "sys-i386-64"
# Use the following value for os variable in case you are running this in a MAC
os_type= "sys-x86-64-sierra"
gazetteer = "geonames"
bounding_box = ""


def geoparser_cmd(text):
    atempt = 0
    flag = 1
    geoparser_xml = ''
    if "-" in text:
        text = text.replace("-", "")
    if "\\" in text:
        text = text.replace("\\", "")
    if "'" in text:
        text = text.replace("'", "\'\\\'\'")

    print("------ NEW %s\n" % text)
    print("\n")

    cmd = 'echo \'%s\' \'' + text + '\' | ' + defoe_path + 'geoparser-1.3/scripts/run -t plain -g ' + gazetteer + ' ' + bounding_box + ' -top | ' + defoe_path + 'geoparser-1.3/bin/' + os_type + '/lxreplace -q s | ' + defoe_path + 'geoparser-1.3/bin/' + os_type + '/lxt -s ' + defoe_path + 'geoparser-1.3/lib/georesolve/addfivewsnippet.xsl'

    print("CMD is %s" % cmd)

    while (len(geoparser_xml) < 5) and (atempt < 10) and (flag == 1):
        proc = subprocess.Popen(cmd.encode('utf-8'), shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        # proc=subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if "Error" in str(stderr):
            flag = 0
            print("err: '{}'".format(stderr))
            print("--------> text is %s" % text)
        else:
            geoparser_xml = stdout
        print(atempt, stdout, stderr)
        atempt += 1
    print("--->Geoparser %s" % geoparser_xml)
    return geoparser_xml


def combine_geoparser_xmls(main_xml, cont_xml, chunk_id):
    if main_xml == "":
        return cont_xml

    main_root = etree.fromstring(main_xml)
    cont_root = etree.fromstring(cont_xml)

    # add all children elements of <p> tag in cont_root to main_root
    main_p_element = main_root.find(".//p")
    cont_p_element = cont_root.find(".//p")
    for cont_p_child in cont_p_element:
        cont_p_child.attrib["chunk"] = chunk_id
        main_p_element.append(cont_p_child)

    # two type of <ents>: <ents source="ner-rb">, <ents source="events">
    # add all children elements of <ents source="ner-rb"> tag in cont_root to main_root
    main_ents_elements = main_root.findall(".//ents")
    cont_ents_elements = cont_root.findall(".//ents")
    main_ents_rb_element = main_ents_elements[0]
    cont_ents_rb_element = cont_ents_elements[0]
    for cont_ents_rb_child in cont_ents_rb_element:
        cont_ents_rb_child.attrib["chunk"] = chunk_id
        main_ents_rb_element.append(cont_ents_rb_child)

    # add all children elements of <ents source="events"> tag in cont_root to main_root
    main_ents_events_element = main_ents_elements[1]
    cont_ents_events_element = cont_ents_elements[1]
    for cont_ents_events_child in cont_ents_events_element:
        cont_ents_events_child.attrib["chunk"] = chunk_id
        main_ents_events_element.append(cont_ents_events_child)

    # add all children elements of <relations> tag in cont_root to main_root
    main_relations_element = main_root.find(".//relations")
    cont_relations_element = cont_root.find(".//relations")
    for cont_relations_child in cont_relations_element:
        cont_relations_child.attrib["chunk"] = chunk_id
        main_relations_element.append(cont_relations_child)
    return etree.tostring(main_root, encoding="utf-8")


def get_geoparser_xml(text, max_length):
    text_chunks = textwrap.wrap(text, max_length, break_long_words=False)
    geoparser_xml = ""
    chunk_count = 0
    for text_chunk in text_chunks:
        geoparser_xml = combine_geoparser_xmls(geoparser_xml, geoparser_cmd(text_chunk), str(chunk_count))
        chunk_count += 1
    return geoparser_xml


def geoparser_coord_xml(geo_xml):
    print(geo_xml)
    dResolvedLocs = dict()
    try:
        root = etree.fromstring(geo_xml)
        for element in root.iter():

            if element.tag == "ent":
                if element.attrib["type"] == "location":
                    latitude = element.attrib["lat"]
                    longitude = element.attrib["long"]
                    toponymId = element.attrib["id"]
                    if "in-country" in element.attrib:
                        in_cc = element.attrib["in-country"]
                    else:
                        in_cc = ''
                    if "pop-size" in element.attrib:
                        pop = element.attrib["pop-size"]
                    else:
                        pop = ''
                    if "feat-type" in element.attrib:
                        type = element.attrib["feat-type"]
                    else:
                        type = ''
                    if "snippet" in element.attrib:
                        snippet_er = element.attrib["snippet"]
                    else:
                        snippet_er = ''
                    for subchild in element:
                        if subchild.tag == "parts":
                            for subsubchild in subchild:
                                toponymName = subsubchild.text
                                # print(toponymName, latitude, longitude)
                                dResolvedLocs[toponymName + "-" + toponymId] = {"lat": latitude, "long": longitude,
                                                                                "pop": pop, "in-cc": in_cc,
                                                                                "type": type, "snippet": snippet_er}
    except Exception as e:
        print(e)
        pass
    return dResolvedLocs


if __name__ == "__main__":

    sentence = "there is nothing here China."
    start_time = time.time()
    dResolvedLocs = geoparser_coord_xml(get_geoparser_xml(sentence, 100000))
    print("result:")
    print(dResolvedLocs)
    end_time = time.time()
    print(end_time - start_time)
    print(get_geoparser_xml(sentence, 1000000))


# 130.7200891971588 for algorithm 1, 5000 max_length
# 130.49024415016174 for algorithm 2, 5000 max_length

# 94.5222589969635 for algorithm 1, 10000 max_length
# 91.43598699569702 for algorithm 2, 10000 max_length
# 78.85387706756592 for algorithm 2, 20000 max_length
# 74.2688980102539 for algorithm 2, 40000 max_length
# 71.02823901176453 for algorithm 2, 80000 max_length
# 68.82072234153748 for algorithm 2, 90000 max_length
# 66.04218578338623 for algorithm 2, 90000 max_length
# 66.13864922523499 for algorithm 1, 100000 max_length
# 65.29441595077515 for algorithm 2, 100000 max_length
# 74.16786813735962 for algorithm 2, 200000 max_length (without splitting text)


