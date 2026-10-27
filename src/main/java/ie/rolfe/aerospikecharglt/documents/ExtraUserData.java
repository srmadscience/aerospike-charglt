/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.rolfe.aerospikecharglt.documents;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that gets stored as JSON data.
 *
 */
public class ExtraUserData  {

    public static final String NEW_LOYALTY_NUMBER = "NEW_LOYALTY_NUMBER";

    public static final String MYSTERIOUS_HEX_PAYLOAD = "mysteriousHexPayload";
    public static final String LOYALTY_SCHEME_NAME = "loyaltySchemeName";
    public static final String LOYALTY_SCHEME_NUMBER = "loyaltySchemeNumber";

    public String mysteriousHexPayload;

    public String loyaltySchemeName;

    public long loyaltySchemeNumber;

    public ExtraUserData(String mysteriousHexPayload, String loyaltySchemeName, long loyaltySchemeNumber) {
        this.mysteriousHexPayload = mysteriousHexPayload;
        this.loyaltySchemeName = loyaltySchemeName;
        this.loyaltySchemeNumber = loyaltySchemeNumber;
    }

    public ExtraUserData() {
    }

    public static ExtraUserData fromJson(Gson gson, String document) {
        return new Gson().fromJson(document, ExtraUserData.class);
    }

    /**
     * Stored as an Aerospike Map bin so individual fields can be changed server side.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(MYSTERIOUS_HEX_PAYLOAD, mysteriousHexPayload);
        map.put(LOYALTY_SCHEME_NAME, loyaltySchemeName);
        map.put(LOYALTY_SCHEME_NUMBER, loyaltySchemeNumber);
        return map;
    }

    public static ExtraUserData fromMap(Map<?, ?> map) {
        if (map == null) {
            return null;
        }
        ExtraUserData eud = new ExtraUserData();
        eud.mysteriousHexPayload = (String) map.get(MYSTERIOUS_HEX_PAYLOAD);
        eud.loyaltySchemeName = (String) map.get(LOYALTY_SCHEME_NAME);
        Object loyaltyNumber = map.get(LOYALTY_SCHEME_NUMBER);
        eud.loyaltySchemeNumber = loyaltyNumber == null ? 0 : ((Number) loyaltyNumber).longValue();
        return eud;
    }

    @Override
    public String toString() {
        return "ExtraUserData{" +
                "mysteriousHexPayload='" + mysteriousHexPayload + '\'' +
                ", loyaltySchemeName='" + loyaltySchemeName + '\'' +
                ", loyaltySchemeNumber=" + loyaltySchemeNumber +
                '}';
    }
}
