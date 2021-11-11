import lombok.Getter;
import lombok.Setter;
import lombok.AllArgsConstructor;
import java.util.*;


@Getter @Setter
public class LSMComponent {
    Node root;
    // will be Btree like

    @AllArgsConstructor
    static class SearchData {
        boolean found;
        int keyIndex;
        int childIndex;
    }

    @Getter @Setter
    static class Node {
        List<KeyData> keyDataList = new ArrayList<KeyData>();
        List<Node> childNodes = new ArrayList<Node>();

        SearchData search(String key) {
            int i = 0;
            for (; i < getKeyDataList().size(); i++) {
                KeyData keyData = getKeyDataList().get(i);
                int compare = keyData.getKey().compareTo(key);
                if (compare < 0) {
                    continue;
                } else if (compare == 0) {
                    return new SearchData(true, i, -1);
                } else {
                    break;
                }
            }

            return new SearchData(false, -1, i);
        }

        Node getChild(int index) {
            // check if leaf.
            return getChildNodes().size() != 0 ? getChildNodes().get(index) : null;
        }
    }

    // TODO:: we need to port this to SBTree search.
    // SBTree root and directory nodes dont contain data, and the keys are just seperators.
    // Lets ignore the seperators aspect and store the appropriate key in the dir nodes instead assuming keys arent arbitrarily large.
    KeyData search(Node node, String key) {
        // same as btree search.
        while (node != null) {
            SearchData searchData = node.search(key);

            if (!searchData.found) {
                node = node.getChild(searchData.childIndex);
            } else {
                return node.getKeyDataList().get(searchData.keyIndex);
            }
        }

        return null;
    }

    // with the lsm inserts and deletes happen as part of the merge process.
    // rules
    // key in c0, key in c1 -- key in c0 wins. heres where a timestamp could help.
    // tombstone in c0, key in c1 -- key gets deleted. if there were more components tombstone will be in c1 as well.
    // key in c0, tombstone in c1 -- key in c0 wins and will overwrite the tombstone.
    //
    void merge() {
    }
}
