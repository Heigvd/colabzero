/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.colabproject.colabzero.api.bean;

import ch.colabproject.colabzero.api.avro.Card;

/**
 *
 * @author maxence
 */
public class CardBean {

    private Long id;

    private String content;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Card fromBean() {
        return Card.newBuilder()
            .setId(this.getId())
            .setContent(this.getContent())
            .build();
    }

    public static CardBean toBean(Card card) {
        CardBean cardBean = new CardBean();
        cardBean.setId(card.getId());
        cardBean.setContent(card.getContent().toString());
        return cardBean;
    }
}
